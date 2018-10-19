# assignment_2018
This application has been deployed in python 2.7 and wrapped in docker.

## Folder Structure

The folder structure is as follows

```
assignment_2018/
  README.md
  Dockerfile
  requirements.txt
  scripts/
    crypto_analysis.py
```

To build and run the application, **these files must exist with exact filenames**:

* `Dockerfile` definition of docker file for the python application;
* `requirements.txt` dependencies that have to be installed.

And then the following commands within docker should be runned:
### `docker build -t crypto-analysis .`
*** This command will deploy the application in docker a container
### `docker run crypto-analysis`
*** This command will run the application and will run the default method `compute_statistics_from_dataset()`
