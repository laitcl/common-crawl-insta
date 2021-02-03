# common-crawl-insta

common-crawl-insta is a tool that analyzes common-crawl warcs and extracts websites that reference instagram links


## Requirements

Having an AWS config is required for the functionality of the the downloader. Please see this [link](https://aws.amazon.com/cli/) to get started with the [AWS-CLI](https://aws.amazon.com/cli/).

This tool is programmed to use postgres. To install postgres, follow this [link](https://www.postgresql.org/download/)
- It is useful to setup your postgres user to have superadmin access.

Spark requires default-jdk, scala, and git to be used. 
`sudo apt install default-jdk scala git -y`

To download and install spark:
```
wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
tar xvf spark-*
sudo mv spark-3.0.1-bin-hadoop2.7 /opt/spark
```

Configure Spark Environment:
```
echo "export SPARK_HOME=/opt/spark" >> ~/.bash_profile
echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.bash_profile
echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.bash_profile
```

## Installation

It is recommended to make a virtual environment before installing python dependencies:
`python3 -m venv ./cc-insta/`

The following activates your virtual environnment:
`source ./cc-insta/bin/activate`

Install all python requirements from requirements.txt using the following:
`pip3 install -r requirements.txt`

## Usage

Download warc files by the following:
`python3 src/cc-downloader.py <prefix/filename>`

Analyze a list of warc rfiles by running the following:
`python3 src/count_domains.py`
The list of warc files to be analyzed are set by the list in line 118
```
archives = [str(PROJECT_DIR)+'/tmp/example.warc.gz']
```

Tests can be run by typing in the following
`pytest`
There are five tests, located in the ./test/ folder.

## Database description

The table `instagram_links` stores all the instagram links, and counts their occurrences in the warc files.

The table `reference_links` stores all the links found in common-crawl that references an instagram link. A warc_date is also saved in this table. If a reference link is found in a warc file that coincides with a reference link in the table `reference_links`, the warc_date will be compared, and the reference link will only be analyzed when the warc_date of the new file is more recent.

The table `address_linked_by` stores all the instances where a reference link links to an instagram link. This table is grouped and counted to generate the counts in the `instagram_links` table.

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)
