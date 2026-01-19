import subprocess
import logging
import re

def get_unbound_stats(command_prefix=["unbound-control"]):
    """
    Executes 'unbound-control stats_noreset' and parses the output into a dictionary.
    Returns a dictionary of stats or None if it fails.
    """
    try:
        cmd = list(command_prefix) + ["stats_noreset"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        stats_output = result.stdout
        
        stats = {}
        for line in stats_output.splitlines():
            if '=' in line:
                key, value = line.split('=', 1)
                try:
                    # Try to convert to float/int if possible
                    if '.' in value:
                        stats[key.strip()] = float(value.strip())
                    else:
                        stats[key.strip()] = int(value.strip())
                except ValueError:
                    stats[key.strip()] = value.strip()
        
        # Calculate derived metrics
        total = stats.get("total.num.queries", 0)
        cache_hits = stats.get("total.num.cachehits", 0)
        cache_misses = stats.get("total.num.cachemiss", 0)
        
        if total > 0:
            stats["cache_hit_rate"] = (cache_hits / total) * 100
        else:
            stats["cache_hit_rate"] = 0.0
            
        # Format uptime if available
        uptime = stats.get("time.up", 0)
        if uptime:
            days = int(uptime // 86400)
            hours = int((uptime % 86400) // 3600)
            minutes = int((uptime % 3600) // 60)
            if days > 0:
                stats["uptime_str"] = f"{days}d {hours}h {minutes}m"
            else:
                stats["uptime_str"] = f"{hours}h {minutes}m"
        else:
            stats["uptime_str"] = "N/A"

        return stats

    except FileNotFoundError:
        logging.warning("unbound-control command not found. Skipping Unbound real-time stats.")
        return None
    except subprocess.CalledProcessError as e:
        logging.warning(f"Error calling unbound-control: {e}. Skipping Unbound real-time stats.")
        return None
    except Exception as e:
        logging.error(f"Unexpected error fetching Unbound stats: {e}")
        return None
