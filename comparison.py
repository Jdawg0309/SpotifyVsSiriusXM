import requests
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error
import os

def get_stock_data(ticker, api_key, months_back=86):
    """Fetch and process historical stock data from Alpha Vantage"""
    end_date = datetime.today()
    start_date = end_date - timedelta(days=30*months_back)
    
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey={api_key}&outputsize=full"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if "Time Series (Daily)" not in data:
            print(f"Error in {ticker} response structure")
            return None

        df = pd.DataFrame(data["Time Series (Daily)"]).T
        df.index = pd.to_datetime(df.index)
        df = df.sort_index(ascending=True)
        
        mask = (df.index >= start_date) & (df.index <= end_date)
        df = df.loc[mask]
        
        column_map = {
            "1. open": "Open",
            "2. high": "High",
            "3. low": "Low",
            "4. close": "Close",
            "5. volume": "Volume"
        }
        
        df = df.rename(columns=column_map)[list(column_map.values())]
        df = df.astype(float)
        df['Ticker'] = ticker  # Add identifier column
        
        return df

    except Exception as e:
        print(f"Error fetching {ticker}: {str(e)}")
        return None

def upload_to_rds_per_service(df, ticker, host, user, password, port=3306):
    """Upload DataFrame to the proper AWS RDS MySQL database per service"""
    # Map ticker to database
    ticker_db_map = {
        'SPOT': 'spotify_service',
        'SIRI': 'siriusxm_service'
    }
    
    database = ticker_db_map.get(ticker)
    if not database:
        print(f"No database mapped for ticker {ticker}")
        return
    
    try:
        # Connect to the corresponding database
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )

        if connection.is_connected():
            cursor = connection.cursor()
            print(f"Connected to {database} at {host}")

            # Insert or update rows into stock_data table
            for index, row in df.iterrows():
                insert_query = """
                INSERT INTO stock_data (date, open, high, low, close, volume, ticker)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    open=VALUES(open),
                    high=VALUES(high),
                    low=VALUES(low),
                    close=VALUES(close),
                    volume=VALUES(volume),
                    ticker=VALUES(ticker)
                """
                cursor.execute(insert_query, (
                    index.date(), 
                    row['Open'], 
                    row['High'], 
                    row['Low'], 
                    row['Close'], 
                    int(row['Volume']), 
                    row['Ticker']
                ))

            connection.commit()
            print(f"Uploaded {len(df)} records to {database}.stock_data")

    except Error as e:
        print(f"Error connecting to MySQL: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection closed")

def save_to_csv(df, ticker):
    """Save DataFrame to CSV with standardized filename"""
    if df is not None and not df.empty:
        filename = f"{ticker}_stock_data_{datetime.today().strftime('%Y%m%d')}.csv"
        df.to_csv(filename)
        print(f"Data saved to {os.path.abspath(filename)}")
        return filename
    return None

def compare_companies(df1, df2):
    """Generate comparative analysis and visualizations"""
    # Merge datasets on date
    combined = pd.merge(df1[['Close']], df2[['Close']], 
                       left_index=True, right_index=True,
                       suffixes=('_SPOT', '_SIRI'))
    
    # Normalize prices for comparison
    combined['SPOT_Norm'] = combined['Close_SPOT'] / combined['Close_SPOT'].iloc[0] * 100
    combined['SIRI_Norm'] = combined['Close_SIRI'] / combined['Close_SIRI'].iloc[0] * 100
    
    # Calculate daily returns
    returns = combined[['Close_SPOT', 'Close_SIRI']].pct_change()
    
    # Create visualizations
    plt.figure(figsize=(12, 6))
    plt.plot(combined.index, combined['SPOT_Norm'], label='Spotify (SPOT)')
    plt.plot(combined.index, combined['SIRI_Norm'], label='SiriusXM (SIRI)')
    plt.title('Normalized Price Performance Comparison (6 Months)')
    plt.ylabel('Normalized Price (Base=100)')
    plt.legend()
    plt.grid(True)
    plt.savefig('spot_vs_siri_performance.png')
    
    # Calculate correlation
    corr = returns.corr().iloc[0,1]
    
    # Print key metrics
    print("\nComparative Analysis:")
    print(f"Correlation of Daily Returns: {corr:.2%}")
    print(f"Spotify Volatility: {returns['Close_SPOT'].std():.2%}")
    print(f"SiriusXM Volatility: {returns['Close_SIRI'].std():.2%}")
    print(f"Spotify Total Return: {(combined['SPOT_Norm'][-1]-100):.2f}%")
    print(f"SiriusXM Total Return: {(combined['SIRI_Norm'][-1]-100):.2f}%")
    
    return combined

# Configuration
API_KEY = "9KFO5THXMSGNE40J"  # Replace with your key
TICKERS = ['SPOT', 'SIRI']

# Main execution
if __name__ == "__main__":
    # AWS RDS Config
    RDS_CONFIG = {
        'host': 'spotifyvssiriusxm.cevge02u6cv8.us-east-1.rds.amazonaws.com',
        'user': 'admin',
        'password': 'Jdawg123',
        'port': 3306  # default MySQL port
    }

    # Fetch data for both companies
    dataframes = {}
    for ticker in TICKERS:
        df = get_stock_data(ticker, API_KEY)
        if df is not None:
            save_to_csv(df, ticker)  # Save to CSV
            upload_to_rds_per_service(df, ticker, **RDS_CONFIG)  # Upload to RDS
            dataframes[ticker] = df

    
    if len(dataframes) == 2:
        # Perform comparison analysis
        comparison_df = compare_companies(dataframes['SPOT'], dataframes['SIRI'])
        comparison_df.to_csv('SPOT_SIRI_comparison.csv')
        print("\nLatest Combined Data:")
        print(comparison_df.tail(3))
    else:
        print("Could not retrieve data for both companies")