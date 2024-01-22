# Get Atlas Node Metrics
Gets metrics for all nodes of a set of Atlas clusters, filterable by org, 
project & cluster. Metrics are obtained via the Atlas CLI. 

# Instructions
1. Change the `CONFIG` variable in `get-atlas-node-metrics.py`:
    - Change `ORG_ID` to point to the Atlas org you want to collect metrics for.
    - Optional: Add project names to `TARGET_PROJECTS` to filter on specific projects.
    - Optional: Add cluster names to `TARGET_CLUSTERS` to filter on specific clusters.
    - Optional: Change `METRICS_PERIOD` and `METRICS_GRANULARITY` to change the 
    period and granularity of the metrics that are collected. Use ISO 8601-formatted date & time. The default is the last 24 hours with 1-minute granularity. 

2. Optional: In the same file as in step 1, (un)comment any metrics that you 
(not) want to collect, in the `PROCESS_METRICS_NAMES` & `DISK_METRICS_NAMES` 
variables.

3. Make sure the Atlas CLI is installed, and you have logged in as a user that
has permissions to access the Atlas org and obtain process & disk metrics.
    - Note that MongoDB employees will need to be connected to VPN when using the
    Atlas CLI.
    - Test the CLI is set up correctly by running:
    `$ atlas projects list --orgId ORG_ID`

4. Run the script: `$ python3 get-atlas-node-metrics.py`
5. Copy the output of the script and paste in a new Google Sheet.
    - While the pasted text is selected, navigate using the main menu to: 
    _Data > Split text to columns_
      - A small dropdown should appear at the bottom of the selected text. Change 
      the _Separator_ to: `Space`
    - Optional: When outputting CPU metrics, select all the CPU columns & rows 
    (by default AY to BD) and navigate using the main menu to: 
    _Format > Number > Number_
