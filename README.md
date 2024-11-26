## Label data using the following scheme:

YYYYMMDD_EID_Position_Name_DailyCount

#### EID = Exercise ID:
- 0: Walk
- 1: Squat
- 2: Sit-Ups
- 3: Bizeps Curl
- 4: Push-Up

#### Position:
- 0: Pocket
- 1: Wrist

#### Name:
- Thang
- Dilara
- Tristan

#### DailyCount:
- A
- B
- ...


#### Example:
- Tristan records squats on 3rd November 2024 for the second time while the sensor is in his pocket
--> 20241103_1_0_Tristan_B

## Instructions for the exercises and sensor positions
- Squats: hands in front of the chest
- Sit-Ups: hands behind the ears
- Sensor should be in the right pocket oder on the right wrist

## Device's Orientation
![Hand's orientation](assets/IMG_0712.PNG)

## How to upload data on Azure
- use the function upload_files_to_blob() in src/dbc.py to upload your data to azure blob storage automatically. This function checks and upload only new .txt files, which doesn't exist in the blob storage. it also creates a new version of the data asset "movements".`

```upload_files_to_blob(path_to_the_data_folder, your_name)```
- path_to_the_data_folder: your folder should only contains the .txt files. 