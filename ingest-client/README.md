## ingest-client [![Circle CI](https://circleci.com/gh/Openthoughts/ingest-client.svg?style=svg&circle-token=b09769767a6d92766ba7f43142292bb4a32686af)](https://circleci.com/gh/Openthoughts/dam-ingest-client)

A java based client application which leverages ingest serive rest api calls to perform Image Ingest operations for OpenThoughts users  

#### Creating a new project

If you haven't already installed [hub](https://github.com/github/hub) go for it!

```bash
# Provide proper substitutions elements wrapped in {....}
$ mkdir ingest-client
$ cd ingest-client
$ cd ingest-client
$ git clone https://github.com/banilbhat/ingest-client.git

```

#### Building your source
```bash
$ gradle
# Creating the ingest client application in eclipse
$ gradle eclipse
# Creating the ingest client application in intellij
$ gradle idea
```

#### Run the application 

```bash   
$ vim gradle/application.gradle
$  replace the line  containing  "args {hotfolderpath}" with "args [yourhotfolderpath]"
$ run gradle
$ java -cp build\libs\ownbackup-1-SNAPSHOT-all.jar com.openthoughts.ingest.client.Main [upload_folder_path]
 ```
   OR
```bash 
 1. Open the application in eclipse or intellij and run from Main.java via eclipse/intellij editor
 2. Go to Constants.java edit the variable  DEFAULT_FOLDER_PATH = {yourhotfolder};
 3. Click on Run -> Run
 ```
####Logging
```bash
Application logs  are located at `current directory with name ingest_client.log

```

