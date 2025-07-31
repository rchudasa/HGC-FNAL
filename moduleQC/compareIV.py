import asyncpg
import asyncio
import argparse
import matplotlib.pyplot as plt
import numpy as np

async def fetch_testing_data(macid, data_type, module_list=None):
    mac_dict = {
        'CMU': {'host': 'cmsmac04.phys.cmu.edu', 'dbname': 'hgcdb'},
        'UCSB': {'host': 'gut.physics.ucsb.edu', 'dbname': 'hgcdb'},
    }
    conn = await asyncpg.connect(
        user='viewer',
        database=mac_dict[macid]['dbname'],
        host=mac_dict[macid]['host']
    )
    
    placeholders = ', '.join(f'${i+1}' for i in range(len(module_list))) if module_list and module_list[0] != 'ALL' else ''
    module_filter = f"WHERE module_name IN ({placeholders})" if module_list and module_list[0] != 'ALL' else ""
    
    # Fetch all mod_iv_test rows, including module_name, voltages, currents, and mod_ivtest_no
    query_mod_iv = f"""SELECT module_name, meas_v, meas_i, mod_ivtest_no 
                       FROM module_iv_test {module_filter} 
                       ORDER BY module_name, mod_ivtest_no;"""
    query_mod_ped = f"""SELECT * FROM module_pedestal_test {module_filter} ORDER BY mod_pedtest_no;"""
    query_mod_qcs = f"""SELECT * FROM module_qc_summary {module_filter} ORDER BY mod_qc_no;"""

    query_type_dict = {
        'mod_iv': query_mod_iv,
        'mod_ped': query_mod_ped,
        'mod_qcs': query_mod_qcs,
    }

    if module_list and module_list[0] == 'ALL':
        rows = await conn.fetch(query_type_dict[data_type])
    else:
        rows = await conn.fetch(query_type_dict[data_type], *module_list)
    await conn.close()
    return rows

async def fetch_all_module_names(macid):
    mac_dict = {
        'CMU': {'host': 'cmsmac04.phys.cmu.edu', 'dbname': 'hgcdb'},
        'UCSB': {'host': 'gut.physics.ucsb.edu', 'dbname': 'hgcdb'},
    }
    conn = await asyncpg.connect(
        user='viewer',
        database=mac_dict[macid]['dbname'],
        host=mac_dict[macid]['host']
    )
    query = """SELECT DISTINCT module_name FROM module_iv_test 
               UNION 
               SELECT DISTINCT module_name FROM module_pedestal_test 
               UNION 
               SELECT DISTINCT module_name FROM module_qc_summary 
               ORDER BY module_name;"""
    rows = await conn.fetch(query)
    await conn.close()
    return [row['module_name'] for row in rows]

def plot_iv_data(rows):
    plt.figure(figsize=(12, 8))
    
    # Use tab20 colormap for up to 20 distinct colors
    for i, row in enumerate(rows):
        module_name = row['module_name']
        voltages = row['meas_v']  # real[] array, returned as a Python list
        currents = row['meas_i']  # real[] array, returned as a Python list
        test_no = row['mod_ivtest_no']
        
        # Ensure arrays are not empty and have the same length
        if voltages and currents and len(voltages) == len(currents):
            color = plt.cm.tab20(i / max(len(rows), 1))  # Assign unique color from tab20 colormap
            plt.plot(
                voltages, 
                currents, 
                marker='o', 
                color=color, 
                label=f'{module_name} (Test {test_no})',
                alpha=0.7
            )
        else:
            print(f"Skipping {module_name} (Test {test_no}): Empty or mismatched voltages/currents arrays")

    plt.xlabel('Voltage (V)')
    plt.ylabel('Current (A)')
    plt.title('IV Curves for All Module Tests')
    plt.legend()
    plt.grid(True)
    plt.show()

async def main():
    parser = argparse.ArgumentParser(description="A script to fetch module data or list all module names from a MAC.")
    parser.add_argument('-dt', '--data_type', default=None, required=False, help="mod_iv, mod_ped, mod_qcs")
    parser.add_argument('-mn', '--module_names', nargs='+', default=None, required=False, help='Module name(s) separated by spaces')
    parser.add_argument('-mac', '--mac', default=None, required=True, help="MAC: CMU, UCSB")
    parser.add_argument('--list-modules', action='store_true', help="List all module names for the specified MAC")
    parser.add_argument('--plot', action='store_true', help="Plot IV data (only for mod_iv data type)")
    args = parser.parse_args()

    if args.list_modules:
        if args.mac != 'CMU':
            print("Module listing is only available for CMU.")
            return
        print(f"Fetching all module names from {args.mac}...")
        module_names = await fetch_all_module_names(args.mac)
        print(f"Found {len(module_names)} modules: {module_names}")
        return

    if not args.data_type:
        print("Error: --data_type is required unless --list-modules is specified.")
        return

    module_names = ['ALL'] if not args.module_names else [mn.upper() for mn in args.module_names]
    print(f'Fetching {args.data_type} for module(s) {module_names} assembled at {args.mac}...')
    rows = await fetch_testing_data(args.mac, args.data_type, module_list=module_names)
    
    if rows:
        if args.plot and args.data_type == 'mod_iv':
            plot_iv_data(rows)
        else:
            # Print data to console
            for row in rows:
                print(dict(row))
    else:
        print("No results found.")

asyncio.run(main())