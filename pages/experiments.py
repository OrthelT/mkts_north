import sqlalchemy as sa
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session
import json
import pymysql
import pandas as pd
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import streamlit as st
import pathlib
from logging_config import setup_logging
import libsql_experimental as libsql

from db_handler import get_local_mkt_engine
from doctrines import create_fit_df, get_fit_summary
logger = setup_logging(__name__, log_file="experiments.log")

mktdb = "wcmkt.db"

icon_id = 0
icon_url = f"https://images.evetech.net/types/{icon_id}/render?size=64"

fit_sqlfile = "Orthel:Dawson007!27608@localhost:3306/wc_fitting"
fit_mysqlfile = "mysql+pymysql://Orthel:Dawson007!27608@localhost:3306/wc_fitting"

@st.cache_resource(ttl=600, show_spinner="Loading libsql connection...")
def get_libsql_connection():
    """Get a connection to the libsql database"""
    return libsql.connect(mktdb)

def get_doctrines_from_db():
    engine = get_local_mkt_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query("SELECT * FROM doctrines", conn)
    return df

def get_doctrine_dict():
    engine = get_local_mkt_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query("SELECT * FROM doctrine_map", conn)

    doctrine_ids = df['doctrine_id'].unique().tolist()
    doctrine_dict = {}
    for id in doctrine_ids:
        df2 = df[df.doctrine_id == id]
        fits = df2['fitting_id'].unique().tolist()
        doctrine_dict[id] = fits
    return doctrine_dict

def get_module_stock_list(module_names: list):
    """Get lists of modules with their stock quantities for display and CSV export."""
    
    # Set the session state variables for the module list and csv module list
    if not st.session_state.get('module_list_state'):
        st.session_state.module_list_state = {}
    if not st.session_state.get('csv_module_list_state'):
        st.session_state.csv_module_list_state = {}

    with Session(get_local_mkt_engine()) as session:
        for module_name in module_names:
            # Check if the module is already in the list, if not, get the data from the database
            if module_name not in st.session_state.module_list_state:
                logger.info(f"Querying database for {module_name}")

                query = f"""
                    SELECT type_name, type_id, total_stock, fits_on_mkt
                    FROM doctrines 
                    WHERE type_name = "{module_name}"
                    LIMIT 1
                """
                result = session.execute(text(query))
                row = result.fetchone()
                if row and row[2] is not None:  # total_stock is now at index 2
                    # Use market stock (total_stock)
                    module_info = f"{module_name} (Total: {int(row[2])} | Fits: {int(row[3])})"
                    csv_module_info = f"{module_name},{row[1]},{int(row[2])},{int(row[3])}\n"
                else:
                    # No quantity if market stock not available
                    module_info = f"{module_name}"
                    csv_module_info = f"{module_name},0,0,0\n"

                # Add the module to the session state list
                st.session_state.module_list_state[module_name] = module_info
                st.session_state.csv_module_list_state[module_name] = csv_module_info

def get_doctrine_lead_ship_id(doctrine_name: str, doctrine_modules: pd.DataFrame) -> int:
    """Get the type ID of the lead ship for a doctrine based on naming conventions."""
    
    # Handle special cases first
    special_cases = {
        'AHAC': 'Zealot',
        'Bomber': 'Purifier', 
        'Tackle': 'Sabre',
        'Retribution': 'Retribution'
    }
    
    # Check for special cases
    doctrine_upper = doctrine_name.upper()
    for key, ship_name in special_cases.items():
        if key in doctrine_upper:
            # Look up the ship ID in the doctrine modules
            ship_data = doctrine_modules[doctrine_modules['ship_name'] == ship_name]
            if not ship_data.empty:
                return ship_data['ship_id'].iloc[0]
    
    # For regular doctrines, extract ship name from doctrine name
    # Most follow pattern like "SUBS - WC Hurricane / WC飓风" where Hurricane is the ship
    if ' - WC ' in doctrine_name:
        # Extract the part after "WC " and before any "/"
        parts = doctrine_name.split(' - WC ')[1]
        if ' / ' in parts:
            ship_name = parts.split(' / ')[0].strip()
        else:
            ship_name = parts.strip()
        
        # Look up the ship ID in the doctrine modules
        ship_data = doctrine_modules[doctrine_modules['ship_name'] == ship_name]
        if not ship_data.empty:
            return ship_data['ship_id'].iloc[0]
    
    # Fallback: try to find any ship in the doctrine modules for this doctrine
    if not doctrine_modules.empty:
        # Get the first ship from this doctrine's modules
        ships_only = doctrine_modules[doctrine_modules['type_name'] == doctrine_modules['ship_name']]
        if not ships_only.empty:
            return ships_only['ship_id'].iloc[0]
        
        # If that fails, just get the first ship_id available
        return doctrine_modules['ship_id'].iloc[0]
    
    # Ultimate fallback - return a default ship ID (e.g., Rifter)
    return 587

def get_fit_name_from_db(fit_id: int) -> str:
    """Get the fit name from the ship_targets table using fit_id."""
    try:
        conn = get_libsql_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT fit_name FROM ship_targets WHERE fit_id = ?", (fit_id,))
        result = cursor.fetchone()
        
        if result:
            fit_name = result[0]
            
            # Handle specific fit name corrections that might get reinserted during DB updates
            fit_name_corrections = {
                # fit_id 39: Correct the incorrect "zz pre2202 WC Hurricane - Drake - Links" name
                39: {
                    "incorrect_patterns": ["zz pre2202 WC Hurricane - Drake - Links"],
                    "correct_name": "WC-EN Shield Links Drake v2.0"
                }
            }
            
            # Check if this fit_id has corrections and apply them if needed
            if fit_id in fit_name_corrections:
                correction_info = fit_name_corrections[fit_id]
                for incorrect_pattern in correction_info["incorrect_patterns"]:
                    if incorrect_pattern in fit_name:
                        logger.info(f"Auto-correcting fit name for fit_id {fit_id}: '{fit_name}' -> '{correction_info['correct_name']}'")
                        
                        # Update the database with the correct name
                        cursor.execute("UPDATE ship_targets SET fit_name = ? WHERE fit_id = ?", 
                                     (correction_info["correct_name"], fit_id))
                        conn.commit()
                        
                        return correction_info["correct_name"]
            
            return fit_name
        else:
            return "Unknown Fit"
    except Exception as e:
        logger.error(f"Error getting fit name for fit_id: {fit_id}")
        logger.error(f"Error: {e}")
        return "Unknown Fit"

def categorize_ship_by_role(ship_name: str) -> str:
    """Categorize ships by their primary fleet role."""
    
    # DPS - Primary damage dealers
    dps_ships = {
        'Hurricane', 'Ferox', 'Zealot', 'Purifier', 'Tornado', 'Oracle', 
        'Harbinger', 'Brutix', 'Myrmidon', 'Talos', 'Naga', 'Rokh',
        'Megathron', 'Hyperion', 'Dominix', 'Raven', 'Scorpion Navy Issue',
        'Raven Navy Issue', 'Typhoon', 'Tempest', 'Maelstrom', 'Abaddon',
        'Apocalypse', 'Armageddon', 'Rifter', 'Punisher', 'Merlin', 'Incursus',
        'Bellicose', 'Deimos', 'Nightmare', 'Retribution', 'Vengeance'
    }
    
    # Logi - Logistics/healing ships
    logi_ships = {
        'Osprey', 'Guardian', 'Basilisk', 'Scimitar', 'Oneiros',
        'Burst', 'Bantam', 'Inquisitor', 'Navitas', 'Zarmazd', 'Deacon', 'Thalia'
    }
    
    # Links - Command ships and fleet booster ships
    links_ships = {
        'Claymore', 'Devoter', 'Drake', 'Cyclone', 'Sleipnir', 'Nighthawk',
        'Damnation', 'Astarte', 'Command Destroyer', 'Bifrost', 'Pontifex',
        'Stork', 'Magus', 'Hecate', 'Confessor', 'Jackdaw', 'Svipul'
    }
    
    # Support - EWAR, tackle, interdiction, etc.
    support_ships = {
        'Sabre', 'Stiletto', 'Malediction', 'Huginn', 'Rapier', 'Falcon',
        'Blackbird', 'Celestis', 'Arbitrator', 'Vigil',
        'Griffin', 'Maulus', 'Crucifier', 'Heretic', 'Flycatcher',
        'Eris', 'Dictor', 'Hictor', 'Broadsword', 'Phobos', 'Onyx',
        'Crow', 'Claw', 'Crusader', 'Taranis', 'Atron', 'Slasher',
        'Executioner', 'Condor'
    }
    
    # Check each category
    if ship_name in dps_ships:
        return "DPS"
    elif ship_name in logi_ships:
        return "Logi"
    elif ship_name in links_ships:
        return "Links"
    elif ship_name in support_ships:
        return "Support"
    else:
        # Default categorization based on ship name patterns
        if any(keyword in ship_name.lower() for keyword in ['hurricane', 'ferox', 'zealot', 'bellicose']):
            return "DPS"
        elif any(keyword in ship_name.lower() for keyword in ['osprey', 'guardian', 'basilisk']):
            return "Logi" 
        elif any(keyword in ship_name.lower() for keyword in ['claymore', 'drake', 'cyclone']):
            return "Links"
        else:
            return "Support"

def display_categorized_doctrine_data(selected_data):
    """Display doctrine data grouped by ship functional roles."""
    
    if selected_data.empty:
        st.warning("No data to display")
        return
    
    # Add role categorization to the dataframe
    selected_data_with_roles = selected_data.copy()
    selected_data_with_roles['role'] = selected_data_with_roles['ship_name'].apply(categorize_ship_by_role)
    
    # Define role colors and emojis for visual appeal
    role_styling = {
        "DPS": {"color": "red", "emoji": "💥", "description": "Primary damage dealers"},
        "Logi": {"color": "green", "emoji": "🏥", "description": "Logistics & healing ships"}, 
        "Links": {"color": "blue", "emoji": "📡", "description": "Command & fleet boost ships"},
        "Support": {"color": "orange", "emoji": "🛠️", "description": "EWAR, tackle & support ships"}
    }
    
    # Group by role and display each category
    roles_present = selected_data_with_roles['role'].unique()
    
    for role in ["DPS", "Logi", "Links", "Support"]:  # Display in logical order
        if role not in roles_present:
            continue
            
        role_data = selected_data_with_roles[selected_data_with_roles['role'] == role]
        style_info = role_styling[role]
        
        # Create expandable section for each role
        with st.expander(
            f"{style_info['emoji']} **{role}** - {style_info['description']} ({len(role_data)} fits)",
            expanded=True
        ):
            # Create columns for metrics summary
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                total_fits = role_data['fits'].sum() if 'fits' in role_data.columns else 0
                st.metric("Total Fits Available", f"{int(total_fits)}")
            
            with col2:
                total_hulls = role_data['hulls'].sum() if 'hulls' in role_data.columns else 0
                st.metric("Total Hulls", f"{int(total_hulls)}")
            
            with col3:
                avg_target_pct = role_data['target_percentage'].mean() if 'target_percentage' in role_data.columns else 0
                st.metric("Avg Target %", f"{int(avg_target_pct)}%")
            
            with col4:
                total_target = role_data['target'].sum() if 'target' in role_data.columns else 0
                st.metric("Total Target", f"{int(total_target)}")
            
            # Display the data table for this role (without the role column)
            display_columns = [col for col in role_data.columns if col != 'role']
            st.dataframe(
                role_data[display_columns],
                use_container_width=True,
                hide_index=True
            )

    

def main():
       # App title and logo
    # Handle path properly for WSL environment
    image_path = pathlib.Path(__file__).parent.parent / "images" / "wclogo.png"
    if image_path.exists():
        st.image(str(image_path), width=150)
    else:
        st.warning("Logo image not found")
    
    # Page title
    st.title("Doctrine Report")
    
    # Fetch the data
    master_df, fit_summary = create_fit_df()

    
    if fit_summary.empty:
        st.warning("No doctrine fits found in the database.")
        return
    
    engine = get_local_mkt_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query("SELECT * FROM doctrine_fits", conn)

    doctrine_names = df.doctrine_name.unique()

    selected_doctrine = st.sidebar.selectbox("Select a doctrine", doctrine_names)
    selected_doctrine_id = df[df.doctrine_name == selected_doctrine].doctrine_id.unique()[0]

    selected_data = fit_summary[fit_summary['fit_id'].isin(df[df.doctrine_name == selected_doctrine].fit_id.unique())]

    # Get module data from master_df for the selected doctrine
    selected_fit_ids = df[df.doctrine_name == selected_doctrine].fit_id.unique()
    doctrine_modules = master_df[master_df['fit_id'].isin(selected_fit_ids)]

    # Create enhanced header with lead ship image    
    # Get lead ship image for this doctrine
    lead_ship_id = get_doctrine_lead_ship_id(selected_doctrine, doctrine_modules)
    lead_ship_image_url = f"https://images.evetech.net/types/{lead_ship_id}/render?size=256"
    
    # Create two-column layout for doctrine header
    header_col1, header_col2 = st.columns([0.2, 0.8], gap="small", vertical_alignment="center")
    
    with header_col1:
        try:
            st.image(lead_ship_image_url, width=128)
        except:
            st.text("🚀 Ship Image Not Available")
    
    with header_col2:
        st.markdown("&nbsp;")  # Add some spacing
        st.subheader(selected_doctrine, anchor=selected_doctrine, divider=True)
        st.markdown("&nbsp;")  # Add some spacing
    
    st.write(f"Doctrine ID: {selected_doctrine_id}")
    # Display categorized doctrine data instead of simple dataframe
    display_categorized_doctrine_data(selected_data)

    # Initialize session state for selected modules
    if 'selected_modules' not in st.session_state:
        st.session_state.selected_modules = []

    # Display lowest stock modules by ship with checkboxes
    if not doctrine_modules.empty:
        st.markdown("---")
        st.markdown("### :blue[Low-Stock Modules]")
        
        # Create two columns for display
        col1, col2 = st.columns(2)
        
        # Get unique fit_ids and process each ship
        for i, fit_id in enumerate(selected_fit_ids):
            fit_data = doctrine_modules[doctrine_modules['fit_id'] == fit_id]
            
            if fit_data.empty:
                continue
                
            # Get ship information
            ship_data = fit_data.iloc[0]
            ship_name = ship_data['ship_name']
            ship_id = ship_data['ship_id']
            
            # Get modules only (exclude the ship hull)
            module_data = fit_data[fit_data['type_name'] != ship_name]
            
            if module_data.empty:
                continue
                
            # Get the 3 lowest stock modules for this ship
            lowest_modules = module_data.sort_values('fits_on_mkt').head(3)
            
            # Determine which column to use
            target_col = col1 if i % 2 == 0 else col2
            
            with target_col:
                # Ship header with image
                ship_image_url = f"https://images.evetech.net/types/{ship_id}/render?size=64"
                
                # Create ship header section
                ship_col1, ship_col2 = st.columns([0.2, 0.8])
                
                with ship_col1:
                    try:
                        st.image(ship_image_url, width=64)
                    except:
                        st.text("🚀")
                
                with ship_col2:
                    # Get fit name from selected_data
                    fit_name = get_fit_name_from_db(fit_id)
                    
                    st.markdown(f"**{ship_name} | {fit_name}**")
                    st.text(f"Fit ID: {fit_id}")
                    st.text(f"Type ID: {ship_id}")
                
                # Display the 3 lowest stock modules
                for _, module_row in lowest_modules.iterrows():
                    module_name = module_row['type_name']
                    module_stock = int(module_row['fits_on_mkt'])
                    module_key = f"ship_module_{fit_id}_{module_name}_{module_stock}"
                    
                    # Get target for this fit from selected_data
                    fit_target_row = selected_data[selected_data['fit_id'] == fit_id]
                    if not fit_target_row.empty and 'ship_target' in fit_target_row.columns:
                        target = fit_target_row['ship_target'].iloc[0]
                    else:
                        target = 20  # Default target
                    
                    # Determine module status based on target comparison with new tier system
                    if module_stock > target * 0.9:
                        badge_status = "On Target"
                        badge_color = "green"
                    elif module_stock > target * 0.2:
                        badge_status = "Needs Attention"
                        badge_color = "orange"
                    else:
                        badge_status = "Critical"
                        badge_color = "red"
                    
                    # Create checkbox and module info
                    checkbox_col, badge_col, text_col = st.columns([0.1, 0.2, 0.7])
                    
                    with checkbox_col:
                        is_selected = st.checkbox(
                            "x", 
                            key=module_key, 
                            label_visibility="hidden",
                            value=module_name in st.session_state.selected_modules
                        )
                        
                        # Update session state based on checkbox
                        if is_selected and module_name not in st.session_state.selected_modules:
                            st.session_state.selected_modules.append(module_name)
                            # Also update the stock info
                            get_module_stock_list([module_name])
                        elif not is_selected and module_name in st.session_state.selected_modules:
                            st.session_state.selected_modules.remove(module_name)
                    
                    with badge_col:
                        # Show badge for all modules to indicate their status
                        st.badge(badge_status, color=badge_color)
                    
                    with text_col:
                        st.text(f"{module_name} ({module_stock})")
                
                # Add spacing between ships
                st.markdown("<br>", unsafe_allow_html=True)

    # Display selected modules if any
    if st.session_state.selected_modules:
        st.markdown("---")
        st.subheader("Low Stock Module List")
        
        # Create columns for display and export
        col1, col2 = st.columns([3, 1])
        
        with col1:
            st.markdown("### Selected Modules:")
            
            # Display modules with their stock information
            for module_name in st.session_state.selected_modules:
                if module_name in st.session_state.get('module_list_state', {}):
                    module_info = st.session_state.module_list_state[module_name]
                    st.text(module_info)
                else:
                    st.text(f"{module_name} (Stock info not available)")
        
        with col2:
            st.markdown("### Export Options")
            
            # Prepare export data
            if st.session_state.get('csv_module_list_state'):
                csv_export = "Type,TypeID,Quantity,Fits\n"
                for module_name in st.session_state.selected_modules:
                    if module_name in st.session_state.csv_module_list_state:
                        csv_export += st.session_state.csv_module_list_state[module_name]
                
                # Download button
                st.download_button(
                    label="📥 Download CSV",
                    data=csv_export,
                    file_name="low_stock_modules.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            # Clear selection button
            if st.button("🗑️ Clear Selection", use_container_width=True):
                st.session_state.selected_modules = []
                st.session_state.module_list_state = {}
                st.session_state.csv_module_list_state = {}
                st.rerun()

if __name__ == "__main__":
    main()