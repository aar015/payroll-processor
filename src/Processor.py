import json
import numpy
import pandas
import re
from datetime import timedelta
from GoogleSheetIOStream import GoogleSheetIOStream


class BonusProcessor (object):
    def __init__(self, iostream, config_dir='config/', working_folder='Chouta Stand Payroll', input_folder='Input', config_folder='Config'):
        self.io_stream = iostream
        self.working_folder = self.io_stream.get_file(name=working_folder)
        self.input_folder = self.io_stream.get_file(name=input_folder, parent=self.working_folder)
        self.config_folder = self.io_stream.get_file(name=config_folder, parent=self.working_folder)
        with open(config_dir + 'format.json', 'r') as format_file:
            self.format = json.load(format_file)
        with open(config_dir + 'bonus_format.json', 'r') as format_file:
            self.bonus_format = json.load(format_file)

    def calc_payroll(self):
        # Calculate hours
        hours_date_range, hours = self.calc_hours()
        # Calculate bonus
        bonus_date_range, bonus = self.calc_bonus()
        # Combine hours and bonuses
        payroll = hours.merge(bonus, how='left', on='Name')
        date_range = [min([hours_date_range[0], bonus_date_range[0]]), max([hours_date_range[1], bonus_date_range[1]])]
        # Upload payroll to googlesheets
        top_line = 'Payroll Period,' + date_range[0] + ',' + date_range[1]
        payroll_name = 'Payroll ' + date_range[0] + ' To ' + date_range[1]
        payroll_data = ''.join([top_line, '\r\n', payroll.to_csv(line_terminator='\r\n', index=False)])
        self.io_stream.upload_sheet(payroll_data, payroll_name, self.working_folder, format=self.format, numlines=len(payroll.index)+1)

    def calc_hours(self):
        # Read in schedule
        hours = self.io_stream.download_sheet('Schedule', self.input_folder)
        hours.loc[:, 'Clock In'] = pandas.to_datetime(hours['Clock In'])
        hours.loc[:, 'Clock Out'] = pandas.to_datetime(hours['Clock Out'])
        # Calculate date date range
        date_range = [min(hours['Clock In']).strftime('%Y-%m-%d'), max(hours['Clock Out']).strftime('%Y-%m-%d')]
        # Read in shops
        shops = self.io_stream.download_sheet('Shops', self.config_folder)
        # Determine conutries shops are if __name__ == '__main__':
        countries = sorted(list(set(shops['Nation'])))
        # Merge hours with shop list
        hours = hours.merge(shops, on='Shop ID')
        # Calculate hours in each nation
        for country in countries:
            hours[country + ' Hours'] = (hours['Clock Out'] - hours['Clock In']) * (hours['Nation'] == country).astype(int) / timedelta(hours=1)
        # Determine columns want in final table
        wanted_columns = ['Name']
        wanted_columns.extend([country + ' Hours' for country in countries])
        # Add up hours from all shifts
        hours = hours[wanted_columns].groupby('Name').sum().reset_index().round(2)
        # Return calculated hours
        return(date_range, hours)

    def calc_bonus(self):
        # Fetch data
        item_record = self.get_item_record()
        trans_record = self.get_trans_record()
        rates = self.io_stream.download_sheet('Bonus Rates', self.config_folder)
        rates.loc[:, 'Start'] = pandas.to_datetime(rates['Start'], errors='coerce').fillna(min(trans_record['Sale Time']))
        rates.loc[:, 'End'] = pandas.to_datetime(rates['End'], errors='coerce').fillna(max(trans_record['Sale Time']))
        # Determine full shop strings
        item_record['Shop'] = item_record['Shop ID'].astype(str) + item_record['Location'] + item_record['Nation']
        trans_record['Shop'] = trans_record['Shop ID'].astype(str) + trans_record['Location'] + trans_record['Nation']
        # Join records with rates
        item_record = self.join_record_rates(item_record, rates)
        trans_record = self.join_record_rates(trans_record, rates, trans=True)
        # Calculate bonuses
        item_record['Bonus'] = item_record['Total Due'] * item_record['Frac of Sale'] + item_record['Per Unit']
        trans_record['Bonus'] = trans_record['Total Due'] * trans_record['Frac of Sale'] + trans_record['Per Unit']
        # Put item bonus and transaction bonus together
        log = item_record.append(trans_record, ignore_index=True)
        log = log[['Name', 'Shop ID', 'Clock In', 'Clock Out', 'Sale Time', 'Line Item', 'Total Due', 'Sales Target', 'Frac of Sale',
                   'Per Unit', 'Bonus', 'Transaction ID']].sort_values(by=['Name', 'Sale Time', 'Transaction ID', 'Total Due']).reset_index(drop=True)
        summary = log[['Name', 'Bonus']].groupby('Name').sum().reset_index()
        # Upload log to google sheets
        date_range = [min(log['Sale Time']).strftime('%Y-%m-%d'), max(log['Sale Time']).strftime('%Y-%m-%d')]
        top_line = 'Bonus Period,' + date_range[0] + ',' + date_range[1]
        log_name = 'Bonus Log ' + date_range[0] + ' To ' + date_range[1]
        log_data = ''.join([top_line, '\r\n', log.to_csv(line_terminator='\r\n', index=False)])
        self.io_stream.upload_sheet(log_data, log_name, self.working_folder, format=self.bonus_format, numlines=len(log.index)+1)
        # Return the summary
        return(date_range, summary)

    def get_item_record(self, shop=None):
        # Read in sales
        sales = self.io_stream.download_sheet('Transactions', self.input_folder)
        sales.loc[:, 'Sale Time'] = pandas.to_datetime(sales['Sale Time'])
        # Read in schedule
        hours = self.io_stream.download_sheet('Schedule', self.input_folder)
        hours.loc[:, 'Clock In'] = pandas.to_datetime(hours['Clock In'])
        hours.loc[:, 'Clock Out'] = pandas.to_datetime(hours['Clock Out'])
        # Read in shops
        shops = self.io_stream.download_sheet('Shops', self.config_folder)
        # Calculate what each person sold
        item_record = self.join_sales_hours(sales, hours)
        # Calculate where each person sold each item
        full_item_record = item_record.merge(shops, on='Shop ID')
        # Return the item record
        return full_item_record[['Name', 'Shop ID', 'Location', 'Nation', 'Clock In', 'Clock Out', 'Sale Time', 'Line Item', 'Total Due', 'Transaction ID']]

    def join_sales_hours(self, sales, hours):
        # Get columns
        sales_time_data = sales['Sale Time'].values
        sales_shop_id = sales['Shop ID'].values
        shift_start_data = hours['Clock In'].values
        shift_end_data = hours['Clock Out'].values
        hours_shop_id = hours['Shop ID'].values
        # Get the indecies where sale time is inbetween clock in and clock out and shop id's are equal
        i, j = numpy.where((sales_time_data[:, None] >= shift_start_data) & (sales_time_data[:, None] <= shift_end_data) & (sales_shop_id[:, None] == hours_shop_id))
        # Drop second shop id so we don't get two of them
        hours = hours.drop(columns='Shop ID')
        # Return the joined data frame
        return pandas.DataFrame(numpy.column_stack([sales.values[i], hours.values[j]]), columns=sales.columns.append(hours.columns))

    def get_trans_record(self, shop=None):
        # Start with an item record
        trans_record = self.get_item_record(shop)
        # Generate code for what was sold in transaction
        trans_record['Full Line Item'] = ' Item ' + trans_record['Line Item']
        # No longer need single line items
        trans_record = trans_record.drop(columns='Line Item')
        # Rename Full Line Item just Line item
        trans_record = trans_record.rename(columns={'Full Line Item': 'Line Item'})
        # Group transactions
        trans_record = trans_record.groupby(['Name', 'Shop ID', 'Location', 'Nation', 'Clock In', 'Clock Out', 'Sale Time', 'Transaction ID']).sum().reset_index()
        # Remove first space from transactions_summary
        trans_record.loc[:, 'Line Item'] = trans_record['Line Item'].str[1:]
        # Return transaction record
        return trans_record[['Name', 'Shop ID', 'Location', 'Nation', 'Clock In', 'Clock Out', 'Sale Time', 'Line Item', 'Total Due', 'Transaction ID']]

    def join_record_rates(self, record, rate, trans=False):
        # Get columns
        record_shop_data = record['Shop'].astype(str).values
        record_sale_time_data = record['Sale Time'].values
        record_line_item_data = record['Line Item'].astype(str).values
        record_total_due_data = record['Total Due'].values
        rate_shop_data = rate['Shop RegExp'].astype(str).values
        rate_start_data = rate['Start'].values
        rate_end_data = rate['End'].values
        if trans:
            rate_line_item_data = rate['Transaction'].astype(str).values
        else:
            rate_line_item_data = rate['Item'].astype(str).values
        rate_target_data = rate['Sales Target'].values
        i = []
        j = []
        # Find indecies
        for row, (record_shop, record_sale_time, record_line_item, record_total_due) in enumerate(zip(record_shop_data, record_sale_time_data, record_line_item_data, record_total_due_data)):
            for col, (rate_shop, rate_start, rate_end, rate_line_item, rate_target) in enumerate(zip(rate_shop_data, rate_start_data, rate_end_data, rate_line_item_data, rate_target_data)):
                if (re.search(rate_shop, record_shop) and record_sale_time >= rate_start and record_sale_time <= rate_end and
                        re.search(rate_line_item, record_line_item) and record_total_due >= rate_target):
                    i.append(row)
                    j.append(col)
        # Return the joined data frame
        return pandas.DataFrame(numpy.column_stack([record.values[i], rate.values[j]]), columns=record.columns.append(rate.columns))


def run():
    io_stream = GoogleSheetIOStream()
    bp = BonusProcessor(io_stream)
    # bp.calc_bonus()
    bp.calc_payroll()


if __name__ == '__main__':
    run()
