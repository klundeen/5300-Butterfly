# 5300-Butterfly
Student DB Relation Manager project for CPSC5300 at Seattle U, Winter 2024

## Usage

`./sql5300 <path_to_db_env>`

While the SQL parser is running, you can execute a simple storage test by calling "test" at the SQL cmd line.

`SQL> test`

## Setup
Clone this repo into a working directory on `cs1`

Set up a directory where the database environment can exist (for example `/cpsc5300/data`)

Additionally, building requires the following libraries (available on `cs1` at `/usr/local/db6`):
- Berkely DB
- Hyrise SQL Parser

## Building
Within the project directory, call make:

`make`

To clean the build and remove binary objects, call make clean:

`make clean`

## Troubleshooting
To enable debug print statments in any cpp file, uncomment the DEBUG_ENABLED definition:

`#define DEBUG_ENABLED`

## Handoff
Milestone 4: [Video Link](https://redhawks-my.sharepoint.com/:v:/g/personal/dvo4_seattleu_edu/Ebs7OQ04tQBHg-ZJ5fIFTE8Ba4DdSCI_CSL0v78PF6ca3g?e=X356EA&nav=eyJyZWZlcnJhbEluZm8iOnsicmVmZXJyYWxBcHAiOiJTdHJlYW1XZWJBcHAiLCJyZWZlcnJhbFZpZXciOiJTaGFyZURpYWxvZy1MaW5rIiwicmVmZXJyYWxBcHBQbGF0Zm9ybSI6IldlYiIsInJlZmVycmFsTW9kZSI6InZpZXcifX0%3D)
note: login with your SU account to view the video

### Notes
- `BTreeIndex::del()` was not implemented