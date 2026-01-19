## Author :  Davis T. Daniel
## PiHoleLongTermStats v.0.2.2
## License :  MIT

import re
import logging
from zoneinfo import ZoneInfo
import pandas as pd


def _is_valid_regex(pattern):
    try:
        re.compile(pattern)
        return True
    except re.error:
        return False


def regex_ignore_domains(df, pattern):
    if _is_valid_regex(pattern):
        mask = df["domain"].str.contains(pattern, regex=True, na=False)
        return df[~mask].reset_index(drop=True)
    else:
        logging.warning(
            f"Ignored invalid regex pattern for domain exclusion : {pattern}"
        )
        return df


def resolve_hostnames(df, hostname_map, display_mode="hostname"):
    """Resolve IP addresses to hostnames based on the hostname mapping
    
    Args:
        df: DataFrame with 'client' column containing IP addresses
        hostname_map: Dictionary mapping IP addresses to hostnames
        display_mode: How to display client information
            - 'hostname': Show hostname (fallback to IP if not available)
            - 'ip': Show IP address only
            - 'both': Show "Hostname (IP)" format
    
    Returns:
        DataFrame with 'client' column updated to display values
        and 'client_ip' column preserving original IP addresses
    """
    logging.info(f"Resolving hostnames with display mode: {display_mode}")
    
    # Preserve original IP addresses in client_ip column
    df["client_ip"] = df["client"]
    
    if display_mode == "ip":
        # Keep IP addresses as-is (no changes needed)
        logging.info("Using IP addresses for client display")
    elif display_mode == "both":
        # Show "Hostname (IP)" or just "IP" if no hostname
        df["client"] = df["client"].apply(
            lambda ip: f"{hostname_map[ip]} ({ip})" if ip in hostname_map else ip
        )
        logging.info("Using 'Hostname (IP)' format for client display")
    else:  # hostname mode (default)
        # Show hostname if available, otherwise show IP
        df["client"] = df["client"].apply(
            lambda ip: hostname_map.get(ip, ip)
        )
        hostnames_resolved = df["client"].ne(df["client_ip"]).sum()
        logging.info(
            f"Resolved {hostnames_resolved} out of {len(df)} queries to hostnames"
        )
    
    return df


def preprocess_df(df, timezone="UTC"):
    """Pre-process df to generate timestamps, blocked,allowed domains etc."""

    logging.info("Pre-processing dataframe...")

    try:
        tz = ZoneInfo(timezone)  # noqa: F841
    except Exception as e:
        logging.warning(f"Invalid timezone '{timezone}', falling back to UTC: {e}")
        timezone = "UTC"

    logging.info(f"Selected timezone : {timezone}")
    df = df.sort_values("timestamp").reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["timestamp"] = df["timestamp"].dt.tz_convert(timezone)
    df["date"] = df["timestamp"].dt.normalize()  # needed in group by operations
    df["hour"] = df["timestamp"].dt.hour
    df["day_period"] = df["hour"].apply(lambda h: "Day" if 6 <= h < 24 else "Night")
    logging.info(
        f"Set timestamp, date, hour and day_period columns using timezone : {timezone}"
    )

    # status ids for pihole ftl db, see pi-hole FTL docs
    logging.info("Processing allowed and blocked status codes...")
    allowed_statuses = [2, 3, 12, 13, 14, 17]
    blocked_statuses = [1, 4, 5, 6, 7, 8, 9, 10, 11, 15, 16, 18]
    df["status_type"] = df["status"].apply(
        lambda x: "Allowed"
        if x in allowed_statuses
        else ("Blocked" if x in blocked_statuses else "Other")
    )

    df["day_name"] = df["timestamp"].dt.day_name()
    df["reply_time"] = pd.to_numeric(df["reply_time"], errors="coerce")
    logging.info("Set status_type, day_name and reply_time columns.")

    return df


def prepare_hourly_aggregated_data(df, n_clients):
    """Pre-aggregate data by hour"""
    logging.info("Pre-aggregating data by hour for callbacks...")

    # aggregate by hour, status_type, and client (which now contains display names)
    hourly_agg = (
        df.groupby([pd.Grouper(key="timestamp", freq="h"), "status_type", "client"])
        .size()
        .reset_index(name="count")
    )

    # get top n_clients clients for client activity view
    top_clients = df["client"].value_counts().nlargest(n_clients).index.tolist()

    logging.info("Hourly aggregation complete")
    return {
        "hourly_agg": hourly_agg,
        "top_clients": top_clients,
    }
