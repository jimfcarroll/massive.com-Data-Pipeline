import datetime
import os
import re
from collections import defaultdict
from typing import Optional, Union
import time
from statistics import mean, median

import pyarrow as pa
from polygon import RESTClient
from polygon.exceptions import BadResponse
from polygon.rest.models.contracts import OptionsContract

import config
from db.connection import DuckDBConnectionWrapper, gen
from utils.parquet import update_parquet_file

# Define the schema
_contracts_schema = pa.schema([
    ("ticker", pa.string()),
    ("cfi", pa.string()),
    ("contract_type", pa.string()),
    ("exercise_style", pa.string()),
    ("expiration_date", pa.date32()),
    ("primary_exchange", pa.string()),
    ("shares_per_contract", pa.int64()),
    ("strike_price", pa.float64()),
])

# Minimum number of contracts needed for a given underlying ticker before switching
# to bulk download mode. When the estimated number of missing contracts for an
# underlying ticker exceeds this threshold, the system will fetch all contracts
# for that underlying instead of fetching contracts individually.
_MIN_TO_USE_BULK_DOWNLOAD = 3

class ContractFetchTimings:
    def __init__(self):
        self.individual_times = []
        self.bulk_fetches = []
        self.last_analysis_time = time.time()
        self.analysis_interval = 100  # Print stats every 100 tickers
        self.tickers_since_last_analysis = 0
    
    def individual(self, fn):
        """Time an individual contract fetch operation
        
        Args:
            fn: Lambda/function that fetches a single contract
            
        Returns:
            Whatever fn returns, after recording the timing
        """
        start_time = time.time()
        result = fn()
        elapsed = time.time() - start_time
        self.add_individual_fetch("unknown", elapsed)  # ticker not needed for stats
        return result
    
    def bulk(self, underlying: str, fn, count):
        """Time a bulk contract fetch operation
        
        Args:
            underlying: The underlying ticker being fetched
            fn: Lambda/function that fetches multiple contracts
            
        Returns:
            Whatever fn returns, after recording the timing
        """
        start_time = time.time()
        result = fn()
        elapsed = time.time() - start_time
        
        # If result is a generator, materialize it to a list
        if hasattr(result, '__iter__') and not isinstance(result, (list, tuple)):
            result = list(result)
            
        self.add_bulk_fetch(underlying, count if count is not None else len(result), elapsed)
        return result
    
    def add_individual_fetch(self, ticker: str, elapsed: float):
        """Record timing for individual contract fetch"""
        self.individual_times.append(elapsed)
        self.tickers_since_last_analysis += 1
        self._maybe_print_analysis()
    
    def add_bulk_fetch(self, underlying: str, num_contracts: int, elapsed: float):
        """Record timing for bulk contract fetch"""
        self.bulk_fetches.append({
            'underlying': underlying,
            'contract_count': num_contracts,
            'total_time': elapsed,
            'time_per_contract': elapsed / num_contracts if num_contracts > 0 else 0
        })
        self._maybe_print_analysis()
    
    def _maybe_print_analysis(self):
        """Print analysis if enough tickers have been processed"""
        if self.tickers_since_last_analysis >= self.analysis_interval:
            self._print_analysis()
            self.tickers_since_last_analysis = 0
    
    def _print_analysis(self):
        """Print current timing statistics"""
        if not self.individual_times or not self.bulk_fetches:
            return
            
        print("\nPerformance Analysis:")
        print(f"Individual Fetches (n={len(self.individual_times)}):")
        print(f"  Average time: {mean(self.individual_times):.3f}s")
        print(f"  Median time: {median(self.individual_times):.3f}s")
        
        bulk_times_per_contract = [m['time_per_contract'] for m in self.bulk_fetches]
        print(f"\nBulk Fetches (n={len(self.bulk_fetches)}):")
        print(f"  Average time per contract: {mean(bulk_times_per_contract):.7f}s")
        print(f"  Median time per contract: {median(bulk_times_per_contract):.7f}s")
        
        # Calculate optimal threshold
        avg_individual = mean(self.individual_times)
        avg_bulk_per_contract = mean(bulk_times_per_contract)
        
        if avg_bulk_per_contract < avg_individual:
            setup_cost = mean([
                m['total_time'] - (m['contract_count'] * avg_bulk_per_contract) 
                for m in self.bulk_fetches
            ])
            crossover = setup_cost / (avg_individual - avg_bulk_per_contract)
            est = int(crossover) + 1
            print(f"\nRecommended _MIN_TO_USE_BULK_DOWNLOAD: {est if est > 3 else 'MINIMUM(3)'}")

def fetch_all_options_contracts(underlying_ticker: str = None, 
                                min_expiration_date_exclusive : Union[str, datetime.date, datetime.datetime ] = None, 
                                expired: bool = True, pagination_limit = 1000):
    """
    Fetches all options contracts for a given underlying ticker from massive.com, including expired contracts.

    Args:
        underlying_ticker (str): The underlying stock ticker (e.g., "AAPL").
        min_expiration_date_exclusive (str, datetime.date, datetime.datetime ): A starting expiration date filter (e.g., "2023-01-01").
        expired (bool): Whether to include expired contracts. Defaults to True.
        pagination_limit (int): The number of results to fetch per API call.

    Yields:
        dict: Each options contract as a dictionary.
    """
    client = RESTClient(config.PAPI_KEY)
    
    # Initialize parameters for the API call
    base_params = {
        "limit": pagination_limit,
        "order": "asc",
        "sort": "expiration_date",
        "strike_price_gt": 0,
        "strike_price_lte": 999999999
    }
    if underlying_ticker is not None:
        base_params["underlying_ticker"] = underlying_ticker
    
    if min_expiration_date_exclusive is not None:
        if isinstance(min_expiration_date_exclusive, str):
            min_expiration_date_exclusive = datetime.datetime.strptime(min_expiration_date_exclusive, '%Y-%m-%d').date()
        elif isinstance(min_expiration_date_exclusive, datetime.datetime):
            min_expiration_date_exclusive = min_expiration_date_exclusive.date()
        elif not isinstance(min_expiration_date_exclusive, datetime.date):
            raise ValueError("expiration_date must be a string in 'YYYY-MM-DD' format, a datetime.date, or a datetime.datetime")

        base_params["expiration_date_gt"] = min_expiration_date_exclusive.isoformat()

    today = datetime.date.today()
    if min_expiration_date_exclusive is not None and min_expiration_date_exclusive >= today:
        expired_params = [ False ]
    else:
        expired_params = [ True, False ] if expired else [ False ]

    try:
        for expired_param in expired_params:
            params = base_params.copy()
            params["expired"] = expired_param
            response = client.list_options_contracts(**params)
            for contract in response:
                # print(f"contract:{contract.ticker}")
                yield contract
        
    except Exception as e:
        print(f"Error fetching options contracts for {underlying_ticker} while {'' if expired else 'without '}including expired: {e}")
        raise

def fetch_options_contract_from_massive(options_ticker: str = None,
                                        as_of: Union[datetime.date, str] = None) -> Optional[OptionsContract]:
    """
    Fetches a single options contract from massive.com by its ticker symbol.
    
    Args:
        options_ticker: The options contract ticker symbol (e.g., "O:AAPL230915C00150000")
        as_of: Optional date to fetch the contract details as they existed on that date.
              Can be either a datetime.date object or a string in 'YYYY-MM-DD' format.
    
    Returns:
        Optional[OptionsContract]: The options contract data if found, None if the contract
                                 doesn't exist or if the API request fails
    
    Note:
        If the API request fails with a BadResponse (e.g., contract not found), 
        the function returns None instead of raising an exception.
    
    Example:
        >>> contract = fetch_options_contract("O:AAPL230915C00150000")
        >>> historical_contract = fetch_options_contract("O:AAPL230915C00150000", "2023-01-01")
    """
    client = RESTClient(config.PAPI_KEY)
    # Initialize parameters for the API call
    params = {
        "ticker": options_ticker
    }
    if as_of is not None:
        params["as_of"] = as_of.isoformat() if isinstance(as_of, datetime.date) else str(as_of)
    
    try:
        return client.get_options_contract(**params)
    except BadResponse as e:
        return None

def to_contract_parquet_file_path(contract: Union[OptionsContract, dict], base_dir: str = None) -> str:
    """
    Generates the parquet file path for storing a given options contract.
    
    Creates a path following the pattern:
    <base_dir>/underlying_ticker=<ticker>/<year>.parquet
    
    Args:
        contract: Either an OptionsContract object or a dictionary containing
                 contract data with 'underlying_ticker' and 'expiration_date' fields
        base_dir: Optional root directory for the parquet files. If None, returns
                 only the relative path
    
    Returns:
        str: Full or relative path to the parquet file where this contract should be stored
    
    Raises:
        ValueError: If contract is missing required 'underlying_ticker' or 'expiration_date' fields
    
    Example:
        >>> contract = {'underlying_ticker': 'AAPL', 'expiration_date': datetime.date(2023, 9, 15)}
        >>> to_contract_parquet_file_path(contract, '/data/contracts')
        '/data/contracts/underlying_ticker=AAPL/2023.parquet'
    """
    if isinstance(contract, OptionsContract):
        contract = options_contract_to_dict(contract, skip_underlying_ticker=False)

    ut = contract.get('underlying_ticker')
    if ut is None:
        raise ValueError(f'Contract "{contract}" is mssing the underlying_ticker field')
    
    ed = contract.get('expiration_date')
    if ed is None:
        raise ValueError(f'Contract "{contract}" is mssing the expiration_date field')

    subdir = os.path.join(f"underlying_ticker={ut}", f"{ed.strftime('%Y')}.parquet")
    return subdir if base_dir is None else os.path.join(base_dir, subdir)

def options_contract_to_dict(contract: OptionsContract, skip_underlying_ticker = False) -> dict:
    """
    Converts an OptionsContract object to a dictionary format suitable for storage.

    Args:
        contract: OptionsContract object to convert
        skip_underlying_ticker: If True, excludes the underlying_ticker field from the output.
                              Used when storing data in partitioned parquet files where
                              underlying_ticker is part of the partition path.
    
    Returns:
        dict: Dictionary containing the following fields:
            - ticker (str): Options contract ticker symbol
            - cfi (str): CFI (Classification of Financial Instruments) code
            - contract_type (str): 'call' or 'put'
            - exercise_style (str): Style of option exercise (e.g., 'american', 'european')
            - expiration_date (datetime.date): Contract expiration date
            - primary_exchange (str): Primary exchange where contract is traded
            - shares_per_contract (int): Number of shares controlled by one contract
            - strike_price (float): Strike price of the option
            - underlying_ticker (str): Ticker of underlying security (omitted if skip_underlying_ticker=True)
    """
    ret = {}
    ret['ticker'] = contract.ticker
    ret['cfi'] = contract.cfi
    ret['contract_type'] = contract.contract_type
    ret['exercise_style'] = contract.exercise_style
    ret['expiration_date'] = datetime.datetime.strptime(contract.expiration_date, '%Y-%m-%d').date()
    ret['primary_exchange'] = contract.primary_exchange
    ret['shares_per_contract'] = contract.shares_per_contract
    ret['strike_price'] = contract.strike_price
    if not skip_underlying_ticker:
        ret['underlying_ticker'] = contract.underlying_ticker

    return ret
 
def parquet_file_exists(con: DuckDBConnectionWrapper, contract: OptionsContract) -> bool:
    """
    Checks if a Parquet file exists for the specified contract.

    Args:
        con: DuckDBConnectionWrapper instance containing directory metadata.
        contract: The contract data to use to see if the parquet file exists.

    Returns:
        True if the file exists, False otherwise.
    """
    pf_path = to_contract_parquet_file_path(contract,con.data_dirs['contracts'])

    # Check if the file exists
    return os.path.exists(pf_path)

def get_options_contract(options_ticker: str = None, 
                         as_of: Union[datetime.date, str] = None) -> Optional[OptionsContract]:
    """
    Retrieves an options contract based on the provided ticker symbol and date.

    Args:
        options_ticker (str): The ticker symbol of the options contract to retrieve.
        as_of (Union[datetime.date, str], optional): The date for which to retrieve the contract.
            If a string is provided, it should be in 'YYYY-MM-DD' format.

    Returns:
        Optional[OptionsContract]: The retrieved options contract, or None if not found.
    """
    if isinstance(as_of, str):
        as_of = datetime.datetime.strptime(as_of, '%Y-%m-%d').date()

    contract = fetch_options_contract_from_massive(options_ticker, as_of - datetime.timedelta(days=1))
    if contract is None:
        contract = fetch_options_contract_from_massive(options_ticker)
    if contract is None:
        contract = parse_ticker(options_ticker)

    print(f"Fetched Individual {contract.underlying_ticker if contract is not None else "NULL"}({options_ticker})")

    return contract


def load_missing_contracts(con: DuckDBConnectionWrapper, table: str):
    """
    Loads missing contracts data from external sources into the database.
    
    Fetches contract data that's missing from the database, either individually
    or in bulk depending on the number of missing contracts for a given underlying.
    
    Args:
        con (DuckDBConnectionWrapper): Database connection wrapper
        table (str): Name of the contracts table
        
    Raises:
        ValueError: If a contract is missing required fields like underlying_ticker
    """
    con.init_views()

    missing_contracts: dict[str, datetime.datetime] = {}

    missing_contracts_count = 0
    print("Querying for missing options tickers ...", flush=True)
    result = con.execute(f"SELECT o.ticker, o.timestamp FROM {table} o LEFT OUTER JOIN contracts c ON o.ticker = c.ticker WHERE c.ticker IS NULL;")
    print(" ... DONE!", flush=True)
    for row in gen(result):
        options_ticker : str = row[0]
        tstamp = row[1]
        if options_ticker in missing_contracts:
            ts = missing_contracts[options_ticker]
            if ts < tstamp:
                missing_contracts[options_ticker] = tstamp
        else:
            missing_contracts[options_ticker] = tstamp
        missing_contracts_count += 1

    # Sort missing_contracts lexicographically by key
    missing_contracts = dict(sorted(missing_contracts.items()))

    print(f"Missing {len(missing_contracts)} contracts paired down from a total result set including {missing_contracts_count} entries")
    underlying_tickers_tried = set()
    timings = ContractFetchTimings()
    
    while True:
        to_remove = set()
        contracts : dict[list[OptionsContract]] = defaultdict(list[OptionsContract])
        for options_ticker, as_of in missing_contracts.items():
            if isinstance(as_of, datetime.datetime):
                as_of = as_of.date()

            # Time individual contract fetch
            contract = timings.individual(
                lambda: get_options_contract(options_ticker, as_of)
            )

            if contract is not None:
                ut = contract.underlying_ticker
                if ut is None:
                    raise ValueError(f'Contract "{contract}" is mssing the underlying_ticker field')

                # add the single contract to the list of contracts and continue
                pf_path = to_contract_parquet_file_path(contract, con.data_dirs['contracts'])
                contracts[pf_path].append(contract)

                # now fetch all of the contracts by this ticker.
                if ut not in underlying_tickers_tried:
                    est_ut_count, earliest_expiration_date = _count_and_earliest_expiration_date(missing_contracts, options_ticker)
                    if not parquet_file_exists(con, contract) or est_ut_count > _MIN_TO_USE_BULK_DOWNLOAD:
                        underlying_tickers_tried.add(ut)

                        print(f"Fetching {est_ut_count} options contracts for {ut} using bulk.", flush=True, end="")

                        # Determine if we need expired contracts based on earliest expiration date
                        today = datetime.date.today()
                        need_expired = earliest_expiration_date <= today if earliest_expiration_date else True

                        # Time bulk fetch
                        bulk_contracts = timings.bulk(ut, 
                            lambda: fetch_all_options_contracts(ut, 
                                expired=need_expired,
                                min_expiration_date_exclusive=(earliest_expiration_date - datetime.timedelta(days=1))
                            )
                        , est_ut_count)
                        if len(bulk_contracts) > 0:
                            # Add all bulk fetched contracts to our contracts dictionary
                            for bulk_contract in bulk_contracts:
                                pf_path = to_contract_parquet_file_path(bulk_contract, con.data_dirs['contracts'])
                                contracts[pf_path].append(bulk_contract)
                            break
                    else:
                        print(f"{ut}({options_ticker})", flush=True)
            else:
                print(f"No contract for {options_ticker}, {as_of}")
                to_remove.add(options_ticker)
        
        for pf_path, ctracts in contracts.items():
            update_parquet_file(pf_path, 
                [options_contract_to_dict(ctract, skip_underlying_ticker=True) for ctract in ctracts], 
                'expiration_date', _contracts_schema, 'ticker')
            for ctract in ctracts:
                if ctract.ticker in missing_contracts:
                    del missing_contracts[ctract.ticker]
        
        for ctick in to_remove:
            if ctick in missing_contracts:
                del missing_contracts[ctick]

        if len(missing_contracts) == 0:
            break

        print(f"{len(missing_contracts)} remaining")

_pattern = r"O:(?P<underlying>[A-Z]+\d*)(?P<expiration>\d{6})(?P<type>[CP])(?P<strike>\d+)"

def parse_ticker(ticker, is_unknown=False) -> dict:
    """
    Attempts to parse an options ticker string into its components.
    
    WARNING: The underlying_ticker extraction is an estimate based on common 
    options ticker formats and may not always match the actual underlying ticker.
    This is especially true for:
    - Complex derivatives
    - Non-standard options
    - Tickers containing numbers
    - Corporate actions (mergers, splits, etc.)
    
    Args:
        ticker: A string representing an options ticker 
               (e.g., "O:AAPL230915C00150000")
               Format: "O:<underlying><expiry><C/P><strike>"
    
    Returns:
        OptionsContract: A contract with the following fields:
            - ticker (str): Original options ticker
            - underlying_ticker (str): Best guess at underlying ticker (may not be accurate)
            - expiration_date (str): Date in YYYY-MM-DD format
            - contract_type (str): 'call' or 'put'
            - strike_price (float): Strike price of the option
            - cfi (str): Always set to 'O'
            - exercise_style (str): Always set to 'MISSING'
            - primary_exchange (str): Always set to 'MISSING' 
            - shares_per_contract (int): Always set to -1
            
    Note:
        The stubbed values (exercise_style, primary_exchange, shares_per_contract) 
        are intentionally set to specific values ('MISSING', 'MISSING', -1). 
        This allows for later querying and correction of derived contracts in 
        the database by filtering on these known stub values.
    """
    match = re.match(_pattern, ticker)
    if not match:
        raise ValueError(f"Invalid ticker format: {ticker}")
    
    underlying = match.group("underlying")
    expiration = match.group("expiration")
    option_type = match.group("type")
    strike = match.group("strike")
    
    # Parse the expiration date
    expiration_date = datetime.date(
        year=2000 + int(expiration[:2]), 
        month=int(expiration[2:4]), 
        day=int(expiration[4:])
    )
    
    # Parse the strike price (divide by 1000 to adjust for scaling)
    strike_price = int(strike) / 1000.0

    # Handle unknown tickers
    record = {
        "ticker": ticker,
        "cfi": "O",  # Default CFI code for options
        "contract_type": "call" if option_type == "C" else "put",
        "exercise_style": "MISSING",
        "expiration_date": expiration_date.isoformat(),
        "primary_exchange": "MISSING",
        "shares_per_contract": -1,
        "strike_price": strike_price,
        "underlying_ticker": underlying,
    }
    # print(record)
    return OptionsContract.from_dict(record)


def _count_and_earliest_expiration_date(missing_contracts: dict[str, datetime.datetime], options_ticker: str) -> tuple[int, Optional[datetime.date]]:
    """
    Counts how many contracts in missing_contracts share the same underlying ticker as the given options_ticker,
    and finds the earliest expiration date among those contracts.
    
    Args:
        missing_contracts: Dictionary mapping options tickers to their timestamps
        options_ticker: The options ticker to analyze and extract the underlying from
        
    Returns:
        tuple[int, Optional[datetime.date]]: A tuple containing:
            - The count of contracts that share the same underlying ticker
            - The earliest expiration date among those contracts, or None if no valid dates found
    """
    parsed = parse_ticker(options_ticker)
    underlying = parsed.underlying_ticker if parsed else None
    print(f"Parsing {options_ticker} gives underlying {underlying}")
    if not underlying:
        return 0, None
        
    count = 0
    earliest_date = None
    
    for ticker in missing_contracts.keys():
        parsed = parse_ticker(ticker)
        if parsed and parsed.underlying_ticker == underlying:
            count += 1
            expiration_date = datetime.datetime.strptime(parsed.expiration_date, '%Y-%m-%d').date() if isinstance(parsed.expiration_date, str) else parsed.expiration_date
            if expiration_date:
                if earliest_date is None or expiration_date < earliest_date:
                    earliest_date = expiration_date

    return count, earliest_date

# Example Usage
if __name__ == "__main__":
    import config

    ticker = "AAPL"
    for contract in fetch_all_options_contracts(config.PAPI_KEY, ticker):
        print(contract)