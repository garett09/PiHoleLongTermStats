## Author :  Davis T. Daniel
## PiHoleLongTermStats v.0.2.2
## License :  MIT

import logging
import pandas as pd
import gc
import plotly.express as px
import itertools


def generate_plot_data(df, n_clients, n_domains):
    """Generate plot data"""

    logging.info("Generating plot data...")

    def shorten(s):
        return s if len(s) <= 45 else f"{s[:20]}...{s[-20:]}"

    top_clients = df["client"].value_counts().nlargest(n_clients).index
    top_clients_stacked = (
        df[df["client"].isin(top_clients)]
        .groupby(["client", "status_type"])
        .size()
        .reset_index(name="count")
    )
    top_clients_stacked["client"] = pd.Categorical(
        top_clients_stacked["client"],
        categories=top_clients_stacked.groupby("client")["count"]
        .sum()
        .sort_values(ascending=False)
        .index,
        ordered=True,
    )
    top_clients_stacked = top_clients_stacked.sort_values(
        ["client", "count"], ascending=[True, False]
    )
    logging.info("Generated plot data for top clients.")

    # plot data for allowed and blocked domains
    tmp_blocked = df[df["status_type"] == "Blocked"].copy()
    tmp_blocked["domain"] = tmp_blocked["domain"].apply(shorten)

    blocked_df = (
        tmp_blocked["domain"]
        .value_counts()
        .nlargest(n_domains)
        .reset_index()
    )
    blocked_df.columns = ["Domain", "count"]

    tmp_allowed = df[df["status_type"] == "Allowed"].copy()
    tmp_allowed["domain"] = tmp_allowed["domain"].apply(shorten)

    allowed_df = (
        tmp_allowed["domain"]
        .value_counts()
        .nlargest(n_domains)
        .reset_index()
    )
    allowed_df.columns = ["Domain", "count"]

    logging.info("Generated plot data for allowed and blocked domains.")

    # plot data for reply time over days
    reply_time_df = (
        df.groupby("date")["reply_time"]
        .mean()
        .mul(1000)
        .reset_index(name="reply_time_ms")
    )

    logging.info("Generated plot data for reply time plot")
    client_list = df["client"].unique().tolist()

    # plot data for doman-client scatter. take minimum from n_domains or n_clients
    top_clients = df["client"].value_counts().nlargest(min(n_domains, n_clients)).index
    top_domains = df["domain"].value_counts().nlargest(min(n_domains, n_clients)).index

    df_top = df.loc[
        df["client"].isin(top_clients) & df["domain"].isin(top_domains)
    ].copy()
    df_top.loc[:, "domain"] = df_top["domain"].apply(shorten)

    client_domain_scatter_df = (
        df_top.groupby(["client", "domain", "status_type"])
        .size()
        .reset_index(name="count")
        .sort_values(by="count")
    )

    # heatmap
    order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]

    day_hour_heatmap = (
        df.groupby(["day_name", "hour"])
        .size()
        .reset_index(name="count")
        .pivot(index="day_name", columns="hour", values="count")
        .fillna(0)
        .reindex(order)
    )

    blocked_day_hour_heatmap = (
        tmp_blocked.groupby(["day_name", "hour"])
        .size()
        .reset_index(name="count")
        .pivot(index="day_name", columns="hour", values="count")
        .fillna(0)
        .reindex(order)
    )

    allowed_day_hour_heatmap = (
        tmp_allowed.groupby(["day_name", "hour"])
        .size()
        .reset_index(name="count")
        .pivot(index="day_name", columns="hour", values="count")
        .fillna(0)
        .reindex(order)
    )

    # plot data for DNS server distribution
    dns_server_df = (
        df["dns_category"]
        .value_counts()
        .reset_index()
    )
    dns_server_df.columns = ["DNS Server", "count"]

    # plot data for query type distribution
    query_type_df = (
        df["query_type"]
        .value_counts()
        .reset_index()
    )
    query_type_df.columns = ["Query Type", "count"]

    del df_top, top_clients, top_domains, tmp_allowed, tmp_blocked
    gc.collect()

    logging.info("Plot data generation complete")

    return {
        "top_clients_stacked": top_clients_stacked,
        "blocked_df": blocked_df,
        "allowed_df": allowed_df,
        "reply_time_df": reply_time_df,
        "client_list": client_list,
        "data_span_days": (df["timestamp"].max() - df["timestamp"].min()).days,
        "client_domain_scatter_df": client_domain_scatter_df,
        "day_hour_heatmap": day_hour_heatmap,
        "blocked_day_hour_heatmap": blocked_day_hour_heatmap,
        "allowed_day_hour_heatmap": allowed_day_hour_heatmap,
        "dns_server_df": dns_server_df,
        "query_type_df": query_type_df,
    }


def generate_queries_over_time(callback_data, client=None):
    dff_grouped = callback_data["hourly_agg"]

    if client is not None:
        logging.info(f"Selected client : {client}")
        dff_grouped = dff_grouped[dff_grouped["client"] == client]
        title_text = f"DNS Queries Over Time for {client}"
    else:
        dff_grouped = (
            dff_grouped.groupby(["timestamp", "status_type"])["count"]
            .sum()
            .reset_index()
        )
        title_text = "DNS Queries Over Time for All Clients"

    if dff_grouped.empty:
        fig = px.area(
            title=f"No activity available for {client}"
            if client
            else "No activity available",
            template="plotly_white",
        )
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Count",
            annotations=[
                dict(
                    text="No data to display",
                    x=0.5,
                    y=0.5,
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                )
            ],
        )
        return fig

    # Fill missing data with 0
    all_times = pd.date_range(
        dff_grouped["timestamp"].min(), dff_grouped["timestamp"].max(), freq="h"
    )
    status_types = ["Other", "Allowed", "Blocked"]
    full_index = pd.MultiIndex.from_product(
        [all_times, status_types], names=["timestamp", "status_type"]
    )
    dff_grouped = (
        dff_grouped.set_index(["timestamp", "status_type"])
        .reindex(full_index, fill_value=0)
        .reset_index()
    )
    dff_grouped["status_type"] = pd.Categorical(
        dff_grouped["status_type"], categories=status_types, ordered=True
    )
    dff_grouped = dff_grouped.sort_values("status_type")

    fig = px.area(
        dff_grouped,
        x="timestamp",
        y="count",
        color="status_type",
        line_group="status_type",
        title=title_text,
        color_discrete_map={
            "Allowed": "#10b981",
            "Blocked": "#ef4444",
            "Other": "#b99529",
        },
        template="plotly_white",
        labels={
            "timestamp": "Date",
            "count": "Count",
            "status_type": "Query Status",
        },
    )

    fig.update_traces(
        mode="lines",
        line_shape="spline",
        line=dict(width=0.5),
        stackgroup="one",
    )

    fig.update_layout(
        legend=dict(orientation="h", yanchor="top", y=-0.4, xanchor="center", x=0.5)
    )

    del dff_grouped
    gc.collect()

    return fig


def generate_client_activity_over_time(callback_data, n_clients, client=None):
    dff_grouped = callback_data["hourly_agg"]
    top_clients = callback_data["top_clients"]

    if client is not None:
        logging.info(f"Selected client : {client}")
        dff_grouped = dff_grouped[dff_grouped["client"] == client]
        dff_grouped = (
            dff_grouped.groupby(["timestamp", "client"])["count"].sum().reset_index()
        )
        title_text = f"Activity for {client}"
        clients_to_show = [client]
    else:
        dff_grouped = dff_grouped[dff_grouped["client"].isin(top_clients)]
        dff_grouped = (
            dff_grouped.groupby(["timestamp", "client"])["count"].sum().reset_index()
        )
        title_text = f"Activity for top {n_clients} clients"
        clients_to_show = top_clients

    if dff_grouped.empty:
        fig = px.area(
            title=f"No activity available for {client}"
            if client
            else "No activity available",
            template="plotly_white",
        )
        fig.update_layout(
            xaxis_title="Date",
            yaxis_title="Count",
            annotations=[
                dict(
                    text="No data to display",
                    x=0.5,
                    y=0.5,
                    xref="paper",
                    yref="paper",
                    showarrow=False,
                )
            ],
        )
        return fig

    all_times = pd.date_range(
        dff_grouped["timestamp"].min(), dff_grouped["timestamp"].max(), freq="h"
    )
    full_index = pd.MultiIndex.from_product(
        [all_times, clients_to_show], names=["timestamp", "client"]
    )
    pivot_df = (
        dff_grouped.set_index(["timestamp", "client"])
        .reindex(full_index, fill_value=0)
        .reset_index()
    )

    default_colors = px.colors.qualitative.Plotly
    client_color_map = dict(zip(top_clients, itertools.cycle(default_colors)))

    fig = px.area(
        pivot_df,
        x="timestamp",
        y="count",
        color="client",
        line_group="client",
        title=title_text,
        color_discrete_map=client_color_map,
        template="plotly_white",
        labels={"timestamp": "Date", "count": "Count", "client": "Client IP"},
    )

    fig.update_traces(
        mode="lines",
        line_shape="spline",
        line=dict(width=0.2),
        stackgroup="one",
        connectgaps=False,
    )

    return fig


def generate_dns_server_pie(callback_data, client=None):
    """Generate DNS server distribution pie chart"""
    df = callback_data["unbound_trend_agg"]
    
    if client is not None:
        df = df[df["client"] == client]

    if df.empty:
        return px.pie(title="No DNS Server Data")

    # Sum counts by category
    df_sum = df.groupby("dns_category")["count"].sum().reset_index()
    df_sum.columns = ["DNS Server", "count"]

    fig = px.pie(
        df_sum,
        values="count",
        names="DNS Server",
        title="DNS Server Distribution" + (f" for {client}" if client else ""),
        color="DNS Server",
        color_discrete_map={
            "Unbound IPv4": "#3498db",  # Standard Blue
            "Unbound IPv6": "#6c5ce7",  # Vibrant Indigo/Purple
            "Cached/Blocked": "#10b981", # Success Green
            "Router": "#f16722",         # Router Orange
            "Other": "#7f8c8d"           # Muted Gray
        },
        template="plotly_white",
        hole=0.4
    )
    
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(showlegend=False)
    
    return fig


def generate_query_type_pie(callback_data, client=None):
    """Generate query type distribution pie chart"""
    df = callback_data["query_type_trend_agg"]
    
    if client is not None:
        df = df[df["client"] == client]

    if df.empty:
        return px.pie(title="No Query Type Data")

    # Sum counts by category
    df_sum = df.groupby("query_type")["count"].sum().reset_index()
    df_sum.columns = ["Query Type", "count"]

    fig = px.pie(
        df_sum,
        values="count",
        names="Query Type",
        title="Query Type Distribution" + (f" for {client}" if client else ""),
        template="plotly_white",
        hole=0.4
    )
    
    fig.update_traces(textposition="inside", textinfo="percent+label")
    fig.update_layout(showlegend=True, legend=dict(orientation="h", yanchor="top", y=-0.1, xanchor="center", x=0.5))
    
    return fig


def generate_unbound_usage_over_time(callback_data, client=None):
    """Generate Unbound IPv4 vs IPv6 usage over time area chart"""
    df = callback_data["unbound_trend_agg"]
    
    if client is not None:
        df = df[df["client"] == client]

    # Filter for only Unbound categories
    df = df[df["dns_category"].isin(["Unbound IPv4", "Unbound IPv6"])].copy()
    
    if df.empty:
        return px.area(title="No Unbound Usage Data Over Time", template="plotly_white")

    # Aggregate by timestamp and category (in case multiple clients are selected or global view)
    df_grouped = df.groupby(["timestamp", "dns_category"])["count"].sum().reset_index()

    fig = px.area(
        df_grouped,
        x="timestamp",
        y="count",
        color="dns_category",
        title="Unbound IPv4 vs IPv6 Usage Over Time" + (f" for {client}" if client else ""),
        color_discrete_map={
            "Unbound IPv4": "#3498db",  # Standard Blue
            "Unbound IPv6": "#6c5ce7",  # Vibrant Indigo/Purple
        },
        template="plotly_white",
        labels={"timestamp": "Time", "count": "Queries", "dns_category": "Server"}
    )
    
    fig.update_traces(mode="lines", line_shape="spline", line=dict(width=0.5), stackgroup="one")
    fig.update_layout(legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5))
    
    return fig


def generate_device_activity_heatmap(callback_data, client=None):
    """Generate a heatmap of device activity (Hour of Day vs Day of Week)"""
    df = callback_data["hourly_agg"]
    
    if client is not None:
        df = df[df["client"] == client]

    if df.empty:
        return px.imshow([[0]], title="No Activity Data for Heatmap")

    # Extract hour and day name from timestamp
    df = df.copy()
    df["hour"] = df["timestamp"].dt.hour
    df["day_name"] = df["timestamp"].dt.day_name()
    
    # Define day order
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    
    # Aggregate by hour and day
    heatmap_df = df.pivot_table(
        index="day_name", 
        columns="hour", 
        values="count", 
        aggfunc="sum",
        fill_value=0
    )
    
    # Reindex to ensure all hours (0-23) and days are present in order
    heatmap_df = heatmap_df.reindex(index=day_order, columns=range(24), fill_value=0)

    fig = px.imshow(
        heatmap_df,
        labels=dict(x="Hour of Day", y="Day of Week", color="Queries"),
        x=heatmap_df.columns,
        y=heatmap_df.index,
        title="Device Activity Pattern" + (f" for {client}" if client else ""),
        color_continuous_scale="Viridis",
        template="plotly_white"
    )
    
    return fig


def generate_unbound_performance_chart(unbound_stats):
    """
    Generate a pie chart showing Unbound cache performance (hits vs misses).
    """
    if not unbound_stats:
        fig = px.pie(title="Unbound Cache Performance (Not Available)")
        fig.update_layout(
            template="plotly_dark",
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
        )
        return fig

    hits = unbound_stats.get("total.num.cachehits", 0)
    misses = unbound_stats.get("total.num.cachemiss", 0)

    df = pd.DataFrame({"Type": ["Cache Hits", "Cache Misses"], "Count": [hits, misses]})

    fig = px.pie(
        df,
        values="Count",
        names="Type",
        title=f"Unbound Cache ({unbound_stats.get('cache_hit_rate', 0):.1f}% Hit Rate)",
        color_discrete_map={"Cache Hits": "#10b981", "Cache Misses": "#ef4444"},
        hole=0.4,
    )

    fig.update_layout(
        template="plotly_white",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=0, t=50, b=0),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        font=dict(color="#2c3e50"),
        title=dict(x=0.5, xanchor="center"),
    )

    fig.update_traces(textposition="inside", textinfo="percent+label")

    return fig
