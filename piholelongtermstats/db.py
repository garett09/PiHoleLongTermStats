## Author :  Davis T. Daniel
## PiHoleLongTermStats v.0.2.2
## License :  MIT

import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import psutil
import pandas as pd
import logging
from zoneinfo import ZoneInfo
import gc


####### reading the database #######
def connect_to_sql(db_path):
    """Connect to an SQL database"""

    if Path(db_path).is_file():
        conn = sqlite3.connect(db_path)
        conn.text_factory = lambda b: b.decode(errors="replace")
        logging.info(f"Connected to SQL database at {db_path}")
        return conn
    else:
        logging.error(
            f"Database file {db_path} not found. Please provide a valid path."
        )
        raise FileNotFoundError(
            f"Database file {db_path} not found. Please provide a valid path."
        )


def probe_sample_df(conn):
    """compute basic stats from a subset of the databases"""

    # calculate safe chunksize to not overload system memory
    sample_query = """SELECT id, timestamp, type, status, domain, client, reply_time
    FROM queries LIMIT 5"""
    sample_df = pd.read_sql_query(sample_query, conn)
    sample_df["timestamp"] = pd.to_datetime(sample_df["timestamp"], unit="s")

    available_memory = psutil.virtual_memory().available
    memory_per_row = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    safe_memory = available_memory * 0.5
    chunksize = int(safe_memory / memory_per_row)
    logging.info(f"Calculated chunksize = {chunksize} based on available memory.")

    latest_ts_raw = pd.read_sql_query("SELECT MAX(timestamp) AS ts FROM queries", conn)[
        "ts"
    ].iloc[0]
    latest_ts = pd.to_datetime(latest_ts_raw, unit="s", utc=True)
    oldest_ts_raw = pd.read_sql_query("SELECT MIN(timestamp) AS ts FROM queries", conn)[
        "ts"
    ].iloc[0]
    oldest_ts = pd.to_datetime(oldest_ts_raw, unit="s", utc=True)

    del sample_df
    gc.collect()

    return chunksize, latest_ts, oldest_ts


def load_hostname_mapping(db_path):
    """Load hostname mapping from network_addresses table
    
    Returns a dictionary mapping IP addresses to hostnames.
    If a hostname is not available, the IP will not be in the mapping.
    """
    conn = connect_to_sql(db_path)
    cursor = conn.cursor()
    
    try:
        # Query to get IP to hostname mapping from network_addresses table
        query = """
        SELECT ip, name 
        FROM network_addresses 
        WHERE name IS NOT NULL AND name != ''
        """
        cursor.execute(query)
        
        # Create mapping dictionary
        hostname_map = {}
        for row in cursor.fetchall():
            ip_address = row[0]
            hostname = row[1]
            hostname_map[ip_address] = hostname
        
        logging.info(f"Loaded {len(hostname_map)} hostname mappings from network_addresses table")
        
    except Exception as e:
        logging.warning(f"Could not load hostname mapping: {e}")
        logging.warning("Hostnames will not be available. Falling back to IP addresses.")
        hostname_map = {}
    
    finally:
        conn.close()
    
    return hostname_map
def load_client_mac_mapping(db_path):
    """Load IP-to-MAC and MAC-to-Hostname mapping from network table
    
    Returns:
        ip_to_mac: dict mapping IP addresses to MAC addresses
        mac_to_name: dict mapping MAC addresses to the best available hostname
    """
    conn = connect_to_sql(db_path)
    cursor = conn.cursor()
    
    ip_to_mac = {}
    mac_to_name = {}
    
    try:
        # 1. Get IP to MAC mapping
        # network_addresses links IPs to network_id (which has the MAC)
        query_ips = """
        SELECT na.ip, n.hwaddr
        FROM network_addresses na
        JOIN network n ON na.network_id = n.id
        WHERE n.hwaddr IS NOT NULL AND n.hwaddr != ''
        """
        cursor.execute(query_ips)
        for ip, mac in cursor.fetchall():
            ip_to_mac[ip] = mac.lower()

        # 2. Get MAC to Hostname mapping
        # We prefer names from network_addresses, then from network table itself if available
        # This query gets all unique MAC/Name pairs, filtered for non-empty names
        query_names = """
        SELECT n.hwaddr, na.name
        FROM network_addresses na
        JOIN network n ON na.network_id = n.id
        WHERE na.name IS NOT NULL AND na.name != ''
        """
        cursor.execute(query_names)
        for mac, name in cursor.fetchall():
            m = mac.lower()
            # If we don't have a name yet for this MAC, or the current one is an IP (failsafe)
            if m not in mac_to_name:
                mac_to_name[m] = name
            # Note: We could implement more complex logic to pick the "best" hostname 
            # if multiple IPs for one MAC have different hostnames.
            
        logging.info(f"Loaded {len(ip_to_mac)} IP-to-MAC mappings and {len(mac_to_name)} MAC-to-Hostname mappings")
        
    except Exception as e:
        logging.warning(f"Could not load MAC mapping: {e}")
        
    finally:
        conn.close()
        
    return ip_to_mac, mac_to_name




def load_forwarder_mapping(db_path):
    """Load DNS forwarder/server mapping from forward_by_id table
    
    Returns a dictionary mapping forwarder IDs to DNS server addresses.
    """
    conn = connect_to_sql(db_path)
    cursor = conn.cursor()
    
    try:
        # Query to get forwarder ID to DNS server mapping
        query = """
        SELECT id, forward 
        FROM forward_by_id
        """
        cursor.execute(query)
        
        # Create mapping dictionary
        forwarder_map = {}
        for row in cursor.fetchall():
            forwarder_id = row[0]
            dns_server = row[1]
            forwarder_map[forwarder_id] = dns_server
        
        logging.info(f"Loaded {len(forwarder_map)} DNS forwarder mappings from forward_by_id table")
        
    except Exception as e:
        logging.warning(f"Could not load forwarder mapping: {e}")
        logging.warning("DNS server analytics will not be available.")
        forwarder_map = {}
    
    finally:
        conn.close()
    
    return forwarder_map


def categorize_dns_server(forward):
    """Categorize DNS servers for better grouping and display
    
    Args:
        forward: DNS server address (e.g., '127.0.0.1#5335', '::1#5335')
    
    Returns:
        Category name for the DNS server
    """
    # Handle NaN, None, or non-string values
    if forward is None or (isinstance(forward, float) and pd.isna(forward)):
        return "Cached/Blocked"
    
    # Convert to string to be safe
    forward = str(forward)
    
    if "127.0.0.1#5335" in forward:
        return "Unbound IPv4"
    elif "::1#5335" in forward:
        return "Unbound IPv6"
    elif "192.168.50.1" in forward or "fe80::ce28:aaff:fe29:f650" in forward:
        return "Router"
    else:
        return forward  # Return as-is for unknown servers


def get_timestamp_range(days, start_date, end_date, timezone, min_date_available=None):

    try:
        tz = ZoneInfo(timezone)
    except Exception:
        logging.warning(f"Invalid timezone '{timezone}', using UTC")
        tz = ZoneInfo("UTC")

    logging.info(f"Selected timezone: {timezone}")

    if start_date is not None and end_date is not None:
        # if dates are selected, use them
        logging.info(
            f"A date range was selected : {start_date} to {end_date} (TZ: {timezone})."
        )

        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)

        start_dt = start_dt.replace(tzinfo=tz)
        end_dt = end_dt.replace(tzinfo=tz)
    elif days == -1 and min_date_available is not None:
        logging.info(f"All Time selected. Using oldest available record: {min_date_available}")
        start_dt = min_date_available
        end_dt = datetime.now(tz)
    else:
        # otherwise use default day given by days (or args.days)
        logging.info(
            f"A date range was not selected. Using default number of days : {days} (TZ: {timezone})."
        )
        end_dt = datetime.now(tz)
        start_dt = end_dt - timedelta(days=days)

    logging.info(
        f"Trying to read data from PiHole-FTL database(s) for the period ranging from {start_dt} to {end_dt} (TZ: {timezone})..."
    )

    start_timestamp = int(start_dt.astimezone(ZoneInfo("UTC")).timestamp())
    end_timestamp = int(end_dt.astimezone(ZoneInfo("UTC")).timestamp())

    logging.info(
        f"Converted dates ranging from {start_dt} to {end_dt} (TZ: {timezone}) to timestamps in UTC : {start_timestamp} to {end_timestamp}"
    )

    return start_timestamp, end_timestamp


def load_device_activity(db_path):
    """Load device activity metrics from the network table
    
    Returns a dictionary mapping MAC addresses to activity metadata.
    """
    conn = connect_to_sql(db_path)
    cursor = conn.cursor()
    
    device_activity = {}
    
    try:
        # Query metrics from network table
        query = """
        SELECT hwaddr, firstSeen, lastQuery, numQueries, macVendor
        FROM network
        WHERE hwaddr IS NOT NULL AND hwaddr != ''
        """
        cursor.execute(query)
        
        for row in cursor.fetchall():
            mac = row[0].lower()
            device_activity[mac] = {
                "first_seen": datetime.fromtimestamp(row[1], tz=ZoneInfo("UTC")) if row[1] else None,
                "last_query": datetime.fromtimestamp(row[2], tz=ZoneInfo("UTC")) if row[2] else None,
                "lifetime_queries": row[3],
                "vendor": row[4] if row[4] else "Unknown"
            }
            
        logging.info(f"Loaded activity metrics for {len(device_activity)} devices from network table")
        
    except Exception as e:
        logging.warning(f"Could not load device activity: {e}")
    
    finally:
        conn.close()
        
    return device_activity


def read_pihole_ftl_db(
    db_paths,
    days=31,
    start_date=None,
    end_date=None,
    chunksize=None,
    timezone="UTC",
    min_date_available=None,
):
    """Read the PiHole FTL database"""

    start_timestamp, end_timestamp = get_timestamp_range(
        days, start_date, end_date, timezone, min_date_available
    )

    logging.info(
        f"Reading data from PiHole-FTL database(s) for timestamps ranging from {start_timestamp} to {end_timestamp} (TZ: UTC)..."
    )

    query = f"""
    SELECT qs.id, qs.timestamp, qs.type, qs.status, d.domain, c.ip as client, qs.reply_time, qs.forward
    FROM query_storage qs
    JOIN client_by_id c ON qs.client = c.id
    JOIN domain_by_id d ON qs.domain = d.id
    WHERE qs.timestamp >= {start_timestamp} AND qs.timestamp < {end_timestamp};
    """

    for db_idx, db_path in enumerate(db_paths):
        logging.info(
            f"Processing database {db_idx + 1}/{len(db_paths)} at {db_path}..."
        )
        conn = connect_to_sql(db_path)

        chunk_num = 0
        for chunk in pd.read_sql_query(query, conn, chunksize=chunksize[db_idx]):
            chunk_num += 1
            logging.info(
                f"Processing dataframe chunk {chunk_num} from database {db_idx + 1} at {db_path}..."
            )
            yield chunk

        conn.close()
