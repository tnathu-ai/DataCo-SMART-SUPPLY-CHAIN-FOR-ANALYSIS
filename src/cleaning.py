	# import libraries general libraries
import pandas as pd
import numpy as np

# ignore DeprecationWarning Error Messages
import warnings

warnings.filterwarnings('ignore')


def whitespace_remover(df):
    """
    The function will remove extra leading and trailing whitespace from the data.
    Takes the data frame as a parameter and checks the data type of each column.
    If the column's datatype is 'Object.', apply strip function; else, it does nothing.
    Use the whitespace_remover() process on the data frame, which successfully removes the extra whitespace from the columns.
    https://www.geeksforgeeks.org/pandas-strip-whitespace-from-entire-dataframe/
    """
    # iterating over the columns
    for i in df.columns:

        # checking datatype of each columns
        if df[i].dtype == 'str':

            # applying strip function on column
            df[i] = df[i].map(str.strip)
        else:
            # if condition is False then it will do nothing.
            pass

def rename_cols(df):
    rename_cols = ['type', 'real_shipping', 'scheduled_shipping',
       'benefit_per_order', 'sales_per_customer', 'delivery_status',
       'late_delivery_risk', 'category_id', 'category_name', 'customer_city',
       'customer_country', 'customer_email', 'customer_fname', 'customer_id',
       'customer_lname', 'customer_password', 'customer_segment',
       'customer_state', 'customer_street', 'customer_zipcode',
       'department_id', 'department_name', 'latitude', 'longitude', 'Market',
       'order_city', 'order_country', 'order_customer_id',
       'order_date', 'order_id', 'order_item_cardprod_id',
       'order_item_discount', 'order_item_discount_rate', 'order_item_id',
       'order_item_product_price', 'order_item_profit_ratio',
       'order_item_quantity', 'sales', 'order_item_total',
       'order_profit_per_order', 'order_region', 'order_state', 'order_status',
       'order_zipcode', 'product_card_id', 'product_category_id',
       'product_description', 'product_image', 'product_name', 'product_price',
       'product_status', 'shipping_date', 'shipping_mode']
    df.columns = rename_cols
    
    
def basic_cleaning(filename):
    df = pd.read_csv(filename, error_bad_lines=False, encoding= 'unicode_escape')
    
    # rename columns follow SQL conventions
    rename_cols(df)
    
    df = df.drop(['product_description'], inplace=True)
    
    # Cast all values inside the dataframe (except the columns' name) into lower case.
    df = df.applymap(lambda s: s.lower() if type(s) == str else s)
    
    # convert columns to the best possible dtypes, object->string
    df = df.convert_dtypes()
    
    # remove extra leading and trailing whitespace 
    whitespace_remover(df)
    
    # print out the shape
    print(f'The shape of the df is (row, column): {df.shape}\n')
    print(f'The list of the National Leagues final columns\' names is: {df.columns.to_list()}\n\n\n')
    return df.head()
    
    
    
def profile_summary(dataset, plot=False):

    pf = pd.DataFrame({'Attribute': "",
                       'Type': "",
                       'Num. Missing Values': [],
                       'Num. Unique Values': [],
                       'Sknewness': [],
                       'Kurtosis': []
                       })

    rows = []

    for attribute in list(dataset.select_dtypes(include=[
            np.number]).columns.values):

        att_type = dataset[attribute].dtype

        unique_values = pd.unique(dataset[attribute])

        num_missing = sum(pd.isnull(dataset[attribute]))

        sk = skew(dataset[attribute].values, axis=None, nan_policy='omit')

        ct = kurtosis(dataset[attribute].values, axis=None, nan_policy='omit')

        row = [attribute, att_type, num_missing, len(unique_values), sk, ct]

        rows.append(row)

    for attribute in list(dataset.select_dtypes(exclude=[
            np.number]).columns.values):

        att_type = dataset[attribute].dtype

        unique_values = pd.unique(dataset[attribute])

        num_missing = sum(pd.isnull(dataset[attribute]))

        sk = "N/A"

        ct = "N/A"

        row = [attribute, att_type, num_missing, len(unique_values), sk, ct]

        rows.append(row)

    for row in rows:

        pf.loc[len(pf)] = row

        if plot:

            print("Frequency plot per attribute")

            for attribute in dataset.columns:

                unique_values = pd.unique(dataset[attribute])

                num_missing = sum(pd.isnull(dataset[attribute]))

                print('Attribute: %s\nNumber of unique values: %d\nNumber '
                      'of missing values: '
                      '%d\nUnique values:' %
                      (attribute, len(unique_values), num_missing))

                print('\nFrequency plot:\n')

                d = (pd.DataFrame(dataset[attribute].value_counts()))

                ax = sns.barplot(x="index", y=attribute,
                                 data=(d).reset_index())

                ax.set(xlabel=attribute, ylabel='count')

                ax.grid(b=True, which='major', color='w', linewidth=1.0)

                ax.set_xticklabels(
                    labels=d.sort_index().index.values, rotation=90)

                plt.show()

    print("Profiling datasets")

    print(pf.to_string())



def write_interim_path(df, csv_name, folder_name): 
    # set the path of the cleaned data to data 
    interim_data_path = os.path.join(os.path.pardir, '..', 'data','interim', folder_name)

    write_interim_path = os.path.join(interim_data_path, csv_name)
    
    # To write the data from the data frame into a file, use the to_csv function.
    df.to_csv(write_interim_path, index=False)
    print(f'cleaned {csv_name} data was successfully saved!\n\n')