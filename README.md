# tap-helpshift

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from the [Helpshift API](https://apidocs.helpshift.com/#/)
- Extracts the following resources:
  - Issue
  - Issues Analytics
  - Messages
  - Apps
  - Agents   
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

Copyright &copy; 2021 Pathlight
