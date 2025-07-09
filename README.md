# mlgroom
Grooming ML job queues

### Getting Started

To create a new groom file and append runs, use the following command:

```bash
mlg-slurm append run_script.sh:RANGE_START-RANGE_END
````

Parameters:
- run_script.sh:
This must be an existing shell script that will be executed for each job in the specified range.
- RANGE_START-RANGE_END:
A range of integer values (inclusive) used to distinguish each individual run. These values can be used inside the script to parameterize behavior.

## Example
```
mlg-slurm append test.sh:1-2
```
This command does the following:

- Creates a groom.yml file if one does not already exist (with default content).
- Appends two job entries using test.sh:
    - One run using the argument 1
    - Another run using the argument 2

Script Behavior:
```bash
#!/bin/bash
# test.sh
INDEX=$1
echo "Running job with index: $INDEX"
# Add more job-specific logic here
```
This setup ensures that each run is unique and can be customized using the index from the specified range.