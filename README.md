# SCOIR Technical Interview for Back-End Engineers
This repo contains an exercise intended for Back-End Engineers.

## Instructions
1. Fork this repo.
1. Using technology of your choice, complete [the assignment](./Assignment.md).
1. Update this README with
    * a `How-To` section containing any instructions needed to execute your program.
    * an `Assumptions` section containing documentation on any assumptions made while interpreting the requirements.
1. Before the deadline, submit a pull request with your solution.

## Expectations
1. Please take no more than 8 hours to work on this exercise. Complete as much as possible and then submit your solution.
1. This exercise is meant to showcase how you work. With consideration to the time limit, do your best to treat it like a production system.

## Installing Spark and Java
1. /usr/bin/ruby -e "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install)"
1. brew cask install adoptopenjdk/openjdk/adoptopenjdk8
1. brew install apache-spark (to verify type pyspark --version in command line)

## To run the test
1. type `spark-submit test.py`.

## Working
1. All the file are expected to be in csv with delimeter as `,`.
1. When the service object is created it will look for files in `input-directory` and `processed`. If it is in `processed` it will be not do anything on that file.
1. Then it iterates over everyfile, and validates the data. Should there be any errors it will be put into error dataframe with error message and we write down that file in `error-directory`. If the error count is zero the we write down it in `output-directory` in json format. All the read and writes are performed by spark in the same generic function.

## Reason for using pyspark
1. Pyspark runs in distributed environment and gives better performance.
1. If noticed I did not loop any rows in a dataframe to validate (normal python have to loop), the advantage of using pyspark over python as it is scalable should there be million rows.