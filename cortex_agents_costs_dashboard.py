# Import python packages
import streamlit as st
import pandas as pd
import datetime
import json
import plotly.express as px
import plotly.graph_objects as go
from snowflake.snowpark.context import get_active_session

# Set page configuration
st.set_page_config(
    page_title="Snowflake Intelligence Cost Dashboard",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #29B5E8, #11567F);
        padding: 2rem 1rem 1rem 1rem;
        border-radius: 0.5rem;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
    }
    
    .main-title {
        font-family: Arial, sans-serif;
        font-weight: bold;
        font-size: 2.5rem;
        color: white;
        margin: 0;
        text-shadow: 0 2px 4px rgba(0,0,0,0.3);
    }
</style>
""", unsafe_allow_html=True)

# Header
header_html = """
<div class="main-header">
    <h1 class="main-title">🤖 SNOWFLAKE INTELLIGENCE COST DASHBOARD</h1>
    <div style="font-size: 1.1rem; opacity: 0.9;">
        Monitor and analyze costs for all Cortex Agents used in your Snowflake account
    </div>
</div>
"""
st.markdown(header_html, unsafe_allow_html=True)

# Get the current credentials
session = get_active_session()

@st.cache_data
def get_snowflake_edition():
    """Get Snowflake edition for cost estimation"""
    try:
        edition_query = """
        SELECT edition
        FROM SNOWFLAKE.ORGANIZATION_USAGE.ACCOUNTS
        WHERE account_name = CURRENT_ACCOUNT_NAME()
        """
        result = session.sql(edition_query).collect()
        if result:
            return result[0]['EDITION']
        return 'STANDARD'
    except Exception:
        return 'STANDARD'

def get_cost_per_credit(edition):
    """Get estimated cost per credit based on edition"""
    costs = {
        'STANDARD': 2.60,
        'ENTERPRISE': 3.90,
        'BUSINESS_CRITICAL': 5.20
    }
    return costs.get(edition.upper(), 2.60)

def format_cost(credits, cost_per_credit):
    """Format cost for display"""
    cost = credits * cost_per_credit
    if cost == 0:
        return "$0.00"
    elif cost < 0.01:
        return f"${cost:.4f}"
    else:
        return f"${cost:.2f}"

def format_credits(credits):
    """Format credits for display"""
    if credits == 0:
        return "0.000"
    elif credits < 0.001:
        return f"{credits:.6f}"
    elif credits < 1:
        return f"{credits:.3f}"
    else:
        return f"{credits:.2f}"

def create_metric(label, value, cost_per_credit, display_mode, help_text=""):
    """Helper function to create metrics with credit/cost toggle"""
    display_value = format_credits(value) if display_mode == "Credits" else format_cost(value, cost_per_credit)
    metric_label = f"{label} {'Credits' if display_mode == 'Credits' else 'Cost'}"
    return st.metric(metric_label, display_value, help=help_text)

def format_dataframe_for_display(df, credit_cols, display_mode, cost_per_credit):
    """Helper function to format dataframes based on display mode"""
    display_df = df.copy()
    
    if display_mode == "Credits":
        for col in credit_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(format_credits)
    else:
        for col in credit_cols:
            if col in display_df.columns:
                cost_col = col.replace('CREDITS', 'COST').replace('_CREDITS', '_COST')
                display_df[cost_col] = display_df[col].apply(lambda x: format_cost(x, cost_per_credit))
                display_df = display_df.drop(col, axis=1)
    
    return display_df

def get_agent_search_services():
    """Extract all Cortex Search services used by agents"""
    agents_data = get_agents()
    all_agent_search_services = set()
    agent_service_mapping = {}
    
    if not agents_data.empty:
        columns = list(agents_data.columns)
        name_col = columns[1] if len(columns) > 1 else columns[0]
        
        for _, agent_row in agents_data.iterrows():
            agent_name = agent_row[name_col]
            tools_info = get_agent_details(agent_name)
            
            for service in tools_info['cortex_search_services']:
                service_name = service['search_service']
                all_agent_search_services.add(service_name)
                if service_name not in agent_service_mapping:
                    agent_service_mapping[service_name] = []
                agent_service_mapping[service_name].append(agent_name)
    
    return all_agent_search_services, agent_service_mapping

def apply_chart_styling(fig, title, x_label, y_label, display_mode):
    """Apply consistent styling to charts"""
    fig.update_layout(
        title=title,
        xaxis_title=x_label,
        yaxis_title=y_label if display_mode == "Credits" else y_label.replace("Credits", "Cost ($)"),
        font_family="Arial"
    )
    return fig

@st.cache_data
def get_warehouse_costs_breakdown(days):
    """Get warehouse costs breakdown for cortex vs non-cortex queries - performance optimized"""
    cost_query = f"""
    WITH cortex_warehouses AS (
      -- Step 1: Quickly identify warehouses with Cortex Analyst activity
      SELECT DISTINCT warehouse_name
      FROM snowflake.account_usage.query_history
      WHERE start_time >= DATEADD(DAY, -{days}, CURRENT_DATE)
        AND warehouse_name IS NOT NULL
        AND query_tag IN ('cortex-agent', 'snowflake-intelligence')
    ), filtered_queries AS (
      -- Step 2: Get only queries from relevant warehouses (much smaller dataset)
      SELECT
        query_id,
        warehouse_name,
        CASE WHEN query_tag IN ('cortex-agent', 'snowflake-intelligence') THEN 1 ELSE 0 END AS is_cortex_query
      FROM snowflake.account_usage.query_history
      WHERE start_time >= DATEADD(DAY, -{days}, CURRENT_DATE)
        AND warehouse_name IN (SELECT warehouse_name FROM cortex_warehouses)
    ), query_with_credits AS (
      -- Step 3: Join credits only for the filtered set (small join)
      SELECT
        fq.warehouse_name,
        fq.is_cortex_query,
        COALESCE(qa.credits_attributed_compute, 0) + COALESCE(qa.credits_used_query_acceleration, 0) AS total_credits
      FROM filtered_queries fq
      INNER JOIN snowflake.account_usage.query_attribution_history qa ON fq.query_id = qa.query_id
    )
    SELECT
      warehouse_name,
      CASE WHEN is_cortex_query = 1 THEN 'Cortex Analyst' ELSE 'Other Queries' END AS query_type,
      COUNT(*) AS query_count,
      SUM(total_credits) AS total_credits
    FROM query_with_credits
    GROUP BY warehouse_name, is_cortex_query
    ORDER BY warehouse_name, is_cortex_query DESC
    """
    
    try:
        result = session.sql(cost_query).to_pandas()
        return result
    except Exception as e:
        st.error(f"Could not fetch warehouse cost data: {str(e)}")
        return pd.DataFrame()

@st.cache_data  
def get_cortex_analyst_usage(days):
    """Get Cortex Analyst usage history"""
    usage_query = f"""
    SELECT
      START_TIME,
      END_TIME,
      REQUEST_COUNT,
      CREDITS,
      USERNAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
    WHERE START_TIME >= DATEADD(DAY, -{days}, CURRENT_DATE)
    ORDER BY START_TIME DESC
    """
    
    try:
        result = session.sql(usage_query).to_pandas()
        return result
    except Exception as e:
        st.error(f"Could not fetch Cortex Analyst usage data: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_cortex_analyst_requests(days):
    """Get Cortex Analyst requests"""
    requests_query = f"""
    SELECT
      timestamp,
      semantic_model_name,
      user_name,
      latest_question,
      feedback
    FROM snowflake.local.CORTEX_ANALYST_REQUESTS_V
    WHERE timestamp >= DATEADD(DAY, -{days}, CURRENT_DATE)
    ORDER BY timestamp DESC
    LIMIT 1000
    """
    
    try:
        result = session.sql(requests_query).to_pandas()
        return result
    except Exception as e:
        st.error(f"Could not fetch Cortex Analyst requests data: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_agents():
    """Get available Cortex Agents"""
    try:
        agents_data = session.sql("""
            SHOW AGENTS IN SCHEMA SNOWFLAKE_INTELLIGENCE.AGENTS
        """).to_pandas()
        return agents_data
    except Exception as e:
        st.error(f"Error fetching agents: {str(e)}")
        return pd.DataFrame()

@st.cache_data
def get_agent_details(agent_name):
    """Get detailed agent information including tools"""
    try:
        describe_query = f"DESCRIBE AGENT SNOWFLAKE_INTELLIGENCE.AGENTS.{agent_name}"
        agent_details = session.sql(describe_query).to_pandas()
        
        if not agent_details.empty:
            # Parse the agent_spec JSON to find tools
            agent_spec_col = list(agent_details.columns)[6]  # agent_spec is typically at index 6
            agent_spec_json = agent_details.iloc[0][agent_spec_col]
            
            if agent_spec_json:
                try:
                    spec = json.loads(agent_spec_json)
                    
                    # Extract all tools info
                    tools_info = {
                        'cortex_analyst_tools': [],
                        'cortex_search_services': []
                    }
                    
                    if 'tools' in spec:
                        for tool in spec['tools']:
                            if 'tool_spec' in tool:
                                tool_spec = tool['tool_spec']
                                tool_type = tool_spec.get('type')
                                tool_name = tool_spec.get('name', 'Unknown')
                                
                                if tool_type == 'cortex_analyst_text_to_sql':
                                    # Get warehouse and semantic view
                                    warehouse = 'Not specified'
                                    semantic_view = 'Not specified'
                                    
                                    if 'tool_resources' in spec and tool_name in spec['tool_resources']:
                                        tool_resource = spec['tool_resources'][tool_name]
                                        semantic_view = tool_resource.get('semantic_view', 'Not specified')
                                        
                                        if 'execution_environment' in tool_resource:
                                            exec_env = tool_resource['execution_environment']
                                            if exec_env.get('type') == 'warehouse':
                                                warehouse = exec_env.get('warehouse', 'Not specified')
                                    
                                    tools_info['cortex_analyst_tools'].append({
                                        'name': tool_name,
                                        'warehouse': warehouse,
                                        'semantic_view': semantic_view
                                    })
                                
                                elif tool_type == 'cortex_search':
                                    # Extract Cortex Search service info from tool_resources
                                    search_service = 'Unknown'
                                    
                                    if 'tool_resources' in spec and tool_name in spec['tool_resources']:
                                        tool_resource = spec['tool_resources'][tool_name]
                                        # The actual search service name is in the 'name' field
                                        search_service = tool_resource.get('name', 'Unknown')
                                        
                                        # If we have the full qualified name, extract just the service name part
                                        if search_service != 'Unknown' and '.' in search_service:
                                            # Extract just the service name (last part after the last dot)
                                            service_name_parts = search_service.split('.')
                                            simple_service_name = service_name_parts[-1]
                                        else:
                                            simple_service_name = search_service
                                    else:
                                        search_service = tool_name
                                        simple_service_name = tool_name
                                    
                                    tools_info['cortex_search_services'].append({
                                        'name': tool_name,
                                        'search_service': simple_service_name,
                                        'full_service_name': search_service
                                    })
                    
                    return tools_info
                    
                except json.JSONDecodeError:
                    return {'cortex_analyst_tools': [], 'cortex_search_services': []}
        
        return {'cortex_analyst_tools': [], 'cortex_search_services': []}
    except Exception as e:
        st.error(f"Error getting agent details: {str(e)}")
        return {'cortex_analyst_tools': [], 'cortex_search_services': []}

@st.cache_data
def get_cortex_search_usage(days):
    """Get Cortex Search usage history"""
    search_query = f"""
    SELECT
      USAGE_DATE,
      DATABASE_NAME,
      SCHEMA_NAME,
      SERVICE_NAME,
      SERVICE_ID,
      CONSUMPTION_TYPE,
      CREDITS,
      MODEL_NAME,
      TOKENS
    FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_DAILY_USAGE_HISTORY
    WHERE USAGE_DATE >= DATEADD(DAY, -{days}, CURRENT_DATE)
    ORDER BY USAGE_DATE DESC, CREDITS DESC
    """
    
    try:
        result = session.sql(search_query).to_pandas()
        return result
    except Exception as e:
        st.error(f"Could not fetch Cortex Search usage data: {str(e)}")
        return pd.DataFrame()

# Get Snowflake edition for cost estimation
edition = get_snowflake_edition()
cost_per_credit = get_cost_per_credit(edition)

# Track which period tabs have been loaded so we only run heavy queries when needed
if 'loaded_periods' not in st.session_state:
    st.session_state['loaded_periods'] = {
        '30d': False
    }

# Sidebar configuration
st.sidebar.header("⚙️ Configuration")

# Display/cost toggle
display_mode = st.sidebar.radio(
    "Display Mode:",
    ["Credits", "Estimated Cost"],
    help=f"Toggle between credits and estimated cost based on {edition} edition"
)

# Edition info
with st.sidebar.expander("💡 Cost Estimation Details", expanded=False):
    st.write(f"**Snowflake Edition:** {edition}")
    st.write(f"**Cost per Credit:** ${cost_per_credit:.2f}")
    st.write("**Note:** Costs are estimates based on typical pricing.")
    st.write("**Actual costs may vary based on your specific contract.**")

# Main content area
st.subheader("💰 Snowflake Intelligence Cost Analysis")

# Expandable info section
with st.expander("📚 Learn more about Snowflake Intelligence costs", expanded=False):
    st.markdown("""
    **Total costs for Snowflake Intelligence is derived from three main components in a pure consumption-based model:**
    
    1. **🎯 Token usage** - charged per million tokens for both input tokens (context and data the agent processes) and output tokens (the agent's responses, queries, and SQL statements)
    
    2. **🔍 Cortex Search costs** - consumption charges based on the size of knowledge base indexes when the agent accesses internal company data
    
    3. **🏭 Warehouse compute costs** - standard Snowflake warehouse charges when the agent executes queries to answer questions
    
    ---
    
    **📊 This dashboard covers all 3 components:**
    - **Cortex Analyst Usage**: Token consumption for text-to-SQL generation
    - **Cortex Search Costs**: Consumption for knowledge base services used by agents
    - **Warehouse Costs**: Compute costs for executing generated queries
    """)

# Create tabs for different time periods and data views
tab1, tab3, tab7, tab30, tab_agents, tab_search, tab5, tab6 = st.tabs([
    "📅 1 Day", "📅 3 Days", "📅 7 Days", "📅 30 Days",
    "🤖 All Agents", "🔍 Cortex Search", "📊 Cortex Analyst Usage", "📋 Raw Requests Data"
])

# Function to render period tab content
def render_period_tab(days, period_name, display_mode, cost_per_credit):
    st.markdown(f"### 📊 Costs for Last {period_name}")
    
    # Get data
    warehouse_data = get_warehouse_costs_breakdown(days)
    cortex_usage_data = get_cortex_analyst_usage(days)
    search_usage_data = get_cortex_search_usage(days)
    
    # Get agent search services for matching
    all_agent_search_services, _ = get_agent_search_services()
    
    # Calculate totals
    warehouse_cortex_credits = 0
    warehouse_other_credits = 0
    cortex_analyst_credits = cortex_usage_data['CREDITS'].sum() if not cortex_usage_data.empty else 0
    cortex_search_credits = 0
    
    if not warehouse_data.empty:
        cortex_mask = warehouse_data['QUERY_TYPE'] == 'Cortex Analyst'
        other_mask = warehouse_data['QUERY_TYPE'] == 'Other Queries'
        warehouse_cortex_credits = warehouse_data[cortex_mask]['TOTAL_CREDITS'].sum()
        warehouse_other_credits = warehouse_data[other_mask]['TOTAL_CREDITS'].sum()
    
    # Calculate Cortex Search credits for agent services only
    if not search_usage_data.empty and all_agent_search_services:
        agent_search_data = search_usage_data[
            search_usage_data['SERVICE_NAME'].isin(all_agent_search_services)
        ]
        cortex_search_credits = agent_search_data['CREDITS'].sum()
    
    # Total Snowflake Intelligence cost = all three components
    total_snowflake_intelligence_credits = warehouse_cortex_credits + cortex_analyst_credits + cortex_search_credits
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        create_metric("💰 Total Snowflake Intelligence", total_snowflake_intelligence_credits, cost_per_credit, display_mode, 
                     "Total credits/cost for all Snowflake Intelligence services: Cortex Analyst + Warehouse + Cortex Search")
    
    with col2:
        create_metric("🤖 Cortex Analyst", cortex_analyst_credits, cost_per_credit, display_mode,
                     "Credits/cost for text-to-SQL generation")
    
    with col3:
        create_metric("🏭 Warehouse", warehouse_cortex_credits, cost_per_credit, display_mode,
                     "Credits/cost for the SQL query execution")
    
    with col4:
        create_metric("🔍 Cortex Search", cortex_search_credits, cost_per_credit, display_mode,
                     "Credits/cost for search services used by Cortex Agents")
    
    # Warehouse breakdown chart and table - only show warehouses with Cortex Analyst activity
    if not warehouse_data.empty:
        # Prepare data for stacked bar chart and filter for warehouses with Cortex Analyst activity
        pivot_data = warehouse_data.pivot(index='WAREHOUSE_NAME', columns='QUERY_TYPE', values='TOTAL_CREDITS').fillna(0)
        
        # Only keep warehouses that have Cortex Analyst credits > 0
        if 'Cortex Analyst' in pivot_data.columns:
            cortex_warehouses = pivot_data[pivot_data['Cortex Analyst'] > 0]
            
            if not cortex_warehouses.empty:
                st.markdown("#### 📈 Warehouse Credits Breakdown")
                
                fig = go.Figure()
                
                # Prepare y-values based on display mode
                cortex_y_values = cortex_warehouses['Cortex Analyst']
                other_y_values = cortex_warehouses['Other Queries'] if 'Other Queries' in cortex_warehouses.columns else pd.Series([0]*len(cortex_warehouses))
                
                if display_mode == "Estimated Cost":
                    cortex_y_values = cortex_y_values * cost_per_credit
                    other_y_values = other_y_values * cost_per_credit
                
                fig.add_trace(go.Bar(
                    name='Cortex Analyst',
                    x=cortex_warehouses.index,
                    y=cortex_y_values,
                    marker_color='#29B5E8'
                ))
                
                if 'Other Queries' in cortex_warehouses.columns:
                    fig.add_trace(go.Bar(
                        name='Other Queries',
                        x=cortex_warehouses.index,
                        y=other_y_values,
                        marker_color='#11567F'
                    ))
                
                apply_chart_styling(fig, f"Warehouse Usage Breakdown - Last {period_name}", "Warehouse", "Credits Used", display_mode)
                fig.update_layout(barmode='stack')
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Detailed breakdown table - one row per warehouse
                st.markdown("#### 📋 Detailed Warehouse Breakdown")
                
                # Create warehouse breakdown table
                table_data = []
                for warehouse in cortex_warehouses.index:
                    cortex_credits = cortex_warehouses.loc[warehouse, 'Cortex Analyst']
                    other_credits = cortex_warehouses.loc[warehouse, 'Other Queries'] if 'Other Queries' in cortex_warehouses.columns else 0
                    
                    # Get query counts efficiently
                    warehouse_rows = warehouse_data[warehouse_data['WAREHOUSE_NAME'] == warehouse]
                    cortex_queries_count = warehouse_rows[warehouse_rows['QUERY_TYPE'] == 'Cortex Analyst']['QUERY_COUNT'].iloc[0] if len(warehouse_rows[warehouse_rows['QUERY_TYPE'] == 'Cortex Analyst']) > 0 else 0
                    other_queries_count = warehouse_rows[warehouse_rows['QUERY_TYPE'] == 'Other Queries']['QUERY_COUNT'].iloc[0] if len(warehouse_rows[warehouse_rows['QUERY_TYPE'] == 'Other Queries']) > 0 else 0
                    
                    table_data.append({
                        'WAREHOUSE_NAME': warehouse,
                        'CORTEX_ANALYST_CREDITS': cortex_credits,
                        'OTHER_CREDITS': other_credits,
                        'CORTEX_ANALYST_QUERIES': cortex_queries_count,
                        'OTHER_QUERIES': other_queries_count
                    })
                
                # Format table for display
                table_df = pd.DataFrame(table_data)
                formatted_df = format_dataframe_for_display(table_df, ['CORTEX_ANALYST_CREDITS', 'OTHER_CREDITS'], display_mode, cost_per_credit)
                st.dataframe(formatted_df, use_container_width=True, hide_index=True)
                
            else:
                st.info(f"💡 No Cortex Analyst activity found for the last {period_name}.")
        else:
            st.info(f"💡 No Cortex Analyst activity found for the last {period_name}.")
    else:
        st.info(f"💡 No warehouse activity found for the last {period_name}.")
    
    # Cortex Search details if any found
    if cortex_search_credits > 0:
        st.markdown("#### 🔍 Cortex Search Services (Used by Agents)")
        agent_search_data = search_usage_data[
            search_usage_data['SERVICE_NAME'].isin(all_agent_search_services)
        ]
        
        if not agent_search_data.empty:
            # Show services used by agents
            service_summary = agent_search_data.groupby('SERVICE_NAME')['CREDITS'].sum().reset_index()
            service_summary = service_summary.sort_values('CREDITS', ascending=False)
            
            formatted_summary = format_dataframe_for_display(service_summary, ['CREDITS'], display_mode, cost_per_credit)
            st.dataframe(formatted_summary, use_container_width=True, hide_index=True)
    
    # Cortex Analyst usage summary
    if not cortex_usage_data.empty:
        st.markdown("#### 🤖 Cortex Analyst Usage Summary")
        col1, col2 = st.columns(2)
        
        with col1:
            total_requests = cortex_usage_data['REQUEST_COUNT'].sum()
            st.metric("Total Requests", f"{total_requests:,}")
        
        with col2:
            unique_users = cortex_usage_data['USERNAME'].nunique()
            st.metric("Unique Users", str(unique_users))

# Render period tabs with lazy loading
with tab1:
    with st.spinner('🚀 Calculating fresh 1-day Snowflake Intelligence insights...'):
        render_period_tab(1, "1 Day", display_mode, cost_per_credit)

with tab3:
    with st.spinner('✨ Crunching 3-day trends...'):
        render_period_tab(3, "3 Days", display_mode, cost_per_credit)

with tab7:
    with st.spinner('✨ Exploring 7-day patterns...'):
        render_period_tab(7, "7 Days", display_mode, cost_per_credit)

with tab30:
    st.markdown("### ⚠️ 30-Day Analysis - Performance Warning")
    st.warning("🐌 **Performance Notice:** The 30-day analysis processes large amounts of data and may take 2-3 minutes to complete.")
    
    if st.session_state['loaded_periods']['30d']:
        with st.spinner('⏳ Aggregating 30-day history... this can take a couple of minutes.'):
            render_period_tab(30, "30 Days", display_mode, cost_per_credit)
    else:
        st.info("👆 Click the button below to load 30-day cost analysis when you are ready.")
        if st.button("🚀 Load 30-Day Analysis", type="primary", help="Click to confirm and start the 30-day analysis"):
            st.session_state['loaded_periods']['30d'] = True
            st.experimental_rerun()

# All Agents Tab
with tab_agents:
    st.markdown("### 🤖 All Cortex Agents in Account")
    
    agents_data = get_agents()
    
    if not agents_data.empty:
        st.write(f"**Total Agents Found:** {len(agents_data)}")
        
        # Get the actual column names
        columns = list(agents_data.columns)
        name_col = columns[1] if len(columns) > 1 else columns[0]
        
        # Agent details expansion
        for _, agent_row in agents_data.iterrows():
            agent_name = agent_row[name_col]
            
            with st.expander(f"🤖 Agent: {agent_name}", expanded=False):
                # Basic agent info
                if len(columns) > 5:
                    comment_col = columns[5]  # 'comment' is at index 5
                    created_col = columns[0]  # 'created_on' is at index 0
                    owner_col = columns[4]    # 'owner' is at index 4
                    
                    comment = agent_row[comment_col]
                    if pd.isna(comment) or not str(comment).strip():
                        comment = 'No description available'
                    st.write(f"**Description:** {comment}")
                    
                    # Convert timestamp to readable date
                    try:
                        created_timestamp = float(agent_row[created_col])
                        created_date = datetime.datetime.fromtimestamp(created_timestamp)
                        st.write(f"**Created:** {created_date.strftime('%Y-%m-%d %H:%M:%S')}")
                    except:
                        st.write(f"**Created:** {agent_row[created_col]}")
                    
                    st.write(f"**Owner:** {agent_row[owner_col]}")
                
                # Get detailed tool configuration
                tools_info = get_agent_details(agent_name)
                
                if tools_info['cortex_analyst_tools']:
                    st.markdown("**🔍 Cortex Analyst Tools:**")
                    for i, tool in enumerate(tools_info['cortex_analyst_tools'], 1):
                        st.write(f"  {i}. **{tool['name']}** - Warehouse: `{tool['warehouse']}`, View: `{tool['semantic_view']}`")
                
                if tools_info['cortex_search_services']:
                    st.markdown("**🔍 Cortex Search Services:**")
                    for i, service in enumerate(tools_info['cortex_search_services'], 1):
                        full_name = service.get('full_service_name', service['search_service'])
                        st.write(f"  {i}. **{service['name']}** - Service: `{service['search_service']}`")
                        if full_name != service['search_service']:
                            st.write(f"     Full path: `{full_name}`")
                    
                
                if not tools_info['cortex_analyst_tools'] and not tools_info['cortex_search_services']:
                    st.info("No Cortex tools configured for this agent.")
        
        # Show all agents table
        st.markdown("#### 📋 All Agents Summary")
        st.dataframe(agents_data, use_container_width=True, hide_index=True)
        
    else:
        st.warning("No Cortex Agents found in your account.")
        st.info("Ensure you have agents deployed in the SNOWFLAKE_INTELLIGENCE.AGENTS schema.")

# Cortex Search Costs Tab
with tab_search:
    st.markdown("### 🔍 Cortex Search Cost Analysis")
    
    period_days = st.selectbox("Select Time Period:", [7, 1, 3, 30], index=0, key="search_period")
    
    # Get search usage data and agent services
    search_usage_data = get_cortex_search_usage(period_days)
    all_agent_search_services, agent_service_mapping = get_agent_search_services()
    
    if not search_usage_data.empty:
        # Filter search usage to only show services used by agents
        agent_search_usage = search_usage_data[
            search_usage_data['SERVICE_NAME'].isin(all_agent_search_services)
        ] if all_agent_search_services else pd.DataFrame()
        
        # Calculate totals
        total_search_credits = agent_search_usage['CREDITS'].sum() if not agent_search_usage.empty else 0
        total_all_search_credits = search_usage_data['CREDITS'].sum()
        
        # Display metrics using helper function
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            create_metric("💰 Agent Search", total_search_credits, cost_per_credit, display_mode,
                         "Cortex Search costs for services used by Cortex Agents")
        
        with col2:
            create_metric("🔍 Total Search", total_all_search_credits, cost_per_credit, display_mode,
                         "Total Cortex Search costs (all services)")
        
        with col3:
            st.metric("🤖 Agent Services", str(len(all_agent_search_services)),
                     help="Number of Cortex Search services used by agents")
        
        with col4:
            st.metric("📊 Total Services", str(search_usage_data['SERVICE_NAME'].nunique()),
                     help="Total number of Cortex Search services in account")
        
        # Show agent-related search services
        if not agent_search_usage.empty:
            st.markdown("#### 🤖 Search Services Used by Agents")
            
            # Group by service and show which agents use it
            service_breakdown = []
            for service_name in agent_search_usage['SERVICE_NAME'].unique():
                service_data = agent_search_usage[agent_search_usage['SERVICE_NAME'] == service_name]
                agents_using = agent_service_mapping.get(service_name, [])
                
                service_breakdown.append({
                    'SERVICE_NAME': service_name,
                    'AGENTS_USING': ', '.join(agents_using),
                    'TOTAL_CREDITS': service_data['CREDITS'].sum(),
                    'USAGE_DAYS': service_data['USAGE_DATE'].nunique()
                })
            
            breakdown_df = pd.DataFrame(service_breakdown)
            breakdown_df = breakdown_df.sort_values('TOTAL_CREDITS', ascending=False)
            
            # Format and display table
            formatted_breakdown = format_dataframe_for_display(breakdown_df, ['TOTAL_CREDITS'], display_mode, cost_per_credit)
            st.dataframe(formatted_breakdown, use_container_width=True, hide_index=True)
            
            # Show raw data for agent services
            st.markdown("#### 📋 Detailed Usage Data (Agent Services Only)")
            formatted_search = format_dataframe_for_display(agent_search_usage, ['CREDITS'], display_mode, cost_per_credit)
            st.dataframe(formatted_search, use_container_width=True, hide_index=True)
        
        else:
            # Debug: Show what services we're looking for vs what's available
            if all_agent_search_services:
                st.warning("⚠️ Found agent search services but no matching usage data.")
                st.markdown("**🔍 Debugging Service Matching:**")
                
                col_debug1, col_debug2 = st.columns(2)
                
                with col_debug1:
                    st.markdown("**Services from Agents:**")
                    for service in sorted(all_agent_search_services):
                        st.write(f"- `{service}`")
                
                with col_debug2:
                    st.markdown("**Available in Usage History:**")
                    if not search_usage_data.empty:
                        available_services = sorted(search_usage_data['SERVICE_NAME'].unique())
                        for service in available_services[:10]:  # Show first 10
                            match_status = "✅" if service in all_agent_search_services else "❌"
                            st.write(f"{match_status} `{service}`")
                        if len(available_services) > 10:
                            st.write(f"... and {len(available_services)-10} more")
                    else:
                        st.write("No usage data available")
            else:
                st.info("No Cortex Agents found with Cortex Search services configured.")
    
    else:
        st.info(f"No Cortex Search usage data found for the last {period_days} days.")

# Cortex Analyst Usage Tab
with tab5:
    st.markdown("### 🤖 Cortex Analyst Usage Details")
    
    period_days = st.selectbox("Select Time Period:", [7, 1, 3, 30], index=0, key="usage_period")
    usage_data = get_cortex_analyst_usage(period_days)
    
    if not usage_data.empty:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_credits = usage_data['CREDITS'].sum()
            create_metric("📊 Total", total_credits, cost_per_credit, display_mode)
        
        with col2:
            st.metric("🔢 Total Requests", f"{usage_data['REQUEST_COUNT'].sum():,}")
        
        with col3:
            st.metric("👥 Unique Users", str(usage_data['USERNAME'].nunique()))
        
        with col4:
            avg_credits_per_request = total_credits / usage_data['REQUEST_COUNT'].sum() if usage_data['REQUEST_COUNT'].sum() > 0 else 0
            create_metric("📊 Avg per Request", avg_credits_per_request, cost_per_credit, display_mode)
        
        # Usage over time chart
        st.markdown("#### 📈 Usage Over Time")
        if len(usage_data) > 1:
            # Prepare chart data based on display mode
            chart_data = usage_data.copy()
            if display_mode == "Estimated Cost":
                chart_data['DISPLAY_VALUES'] = chart_data['CREDITS'] * cost_per_credit
                y_column = 'DISPLAY_VALUES'
                title = "Cortex Analyst Cost Over Time"
            else:
                chart_data['DISPLAY_VALUES'] = chart_data['CREDITS']
                y_column = 'DISPLAY_VALUES'
                title = "Cortex Analyst Credits Over Time"
            
            fig = px.line(
                chart_data,
                x='START_TIME',
                y=y_column,
                title=title,
                color_discrete_sequence=['#29B5E8']
            )
            apply_chart_styling(fig, title, "Time", "Credits Used", display_mode)
            st.plotly_chart(fig, use_container_width=True)
        
        # Detailed usage table
        st.markdown("#### 📋 Detailed Usage History")
        formatted_usage = format_dataframe_for_display(usage_data, ['CREDITS'], display_mode, cost_per_credit)
        st.dataframe(formatted_usage, use_container_width=True, hide_index=True)
    else:
        st.info(f"💡 No Cortex Analyst usage found for the last {period_days} days.")

# Raw Requests Data Tab  
with tab6:
    st.markdown("### 📋 Cortex Analyst Requests Raw Data")
    
    period_days = st.selectbox("Select Time Period:", [7, 1, 3, 30], index=0, key="requests_period")
    requests_data = get_cortex_analyst_requests(period_days)
    
    if not requests_data.empty:
        st.write(f"**Total Requests:** {len(requests_data):,}")
        st.write(f"**Date Range:** Last {period_days} days")
        
        # Summary by semantic model
        st.markdown("#### 📊 Requests by Semantic Model")
        semantic_col = None
        for col in ['semantic_model_name', 'SEMANTIC_MODEL_NAME']:
            if col in requests_data.columns:
                semantic_col = col
                break
        
        if semantic_col and not requests_data[semantic_col].isna().all():
            model_summary = requests_data[requests_data[semantic_col].notna()].groupby([semantic_col]).size().reset_index(name='REQUEST_COUNT')
            model_summary = model_summary.sort_values('REQUEST_COUNT', ascending=False)
            st.dataframe(model_summary, use_container_width=True, hide_index=True)
        else:
            st.info("No semantic model data available.")
        
        # Summary by user
        st.markdown("#### 👤 Requests by User")
        user_col = None
        for col in ['user_name', 'USER_NAME']:
            if col in requests_data.columns:
                user_col = col
                break
                
        if user_col and not requests_data[user_col].isna().all():
            user_summary = requests_data[requests_data[user_col].notna()].groupby([user_col]).size().reset_index(name='REQUEST_COUNT')
            user_summary = user_summary.sort_values('REQUEST_COUNT', ascending=False)
            st.dataframe(user_summary, use_container_width=True, hide_index=True)
        else:
            st.info("No user data available.")
        
        # Raw requests table
        st.markdown("#### 📋 Raw Requests Data")
        st.dataframe(requests_data, use_container_width=True, hide_index=True)
    else:
        st.info(f"💡 No Cortex Analyst requests found for the last {period_days} days.")

# Footer
st.markdown("---")
st.markdown(f"""
<div style="text-align: center; color: #666; font-size: 0.9rem;">
    <p>💡 <strong>Tip:</strong> Costs include Cortex Analyst text-to-SQL generation, warehouse compute execution, and Cortex Search services used by agents.</p>
    <p>📊 Cost estimates based on {edition} edition (${cost_per_credit:.2f}/credit). Actual costs may vary.</p>
    <p>🕒 Data is sourced from ACCOUNT_USAGE views with up to 3-hour latency.</p>
</div>
""", unsafe_allow_html=True)