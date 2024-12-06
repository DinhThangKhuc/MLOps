import re


def extract_data_from_filename(filename: str) -> dict:
    """ Extract data from a filename using a regular expression pattern.

    The expected filename format is: 'YYYYMMDD_EID_POSITION_NAME_DAILYCOUNT.txt'

    Returns: A dictionary containing the extracted data.
        {"date": "YYYYMMDD", 
        "exercise": "Exercise Name", 
        "position": "Position Name", 
        "name": "Name", 
        "daily_count": "Daily Count"}
    """
    # Define mappings based on the provided information
    exercise_mapping = {
        0: "Walk",
        1: "Squat",
        2: "Sit-Ups",
        3: "Bizeps Curl",
        4: "Push-Up"
    }

    position_mapping = {
        0: "Pocket",
        1: "Wrist"
    }

    # Regular expression pattern to match the filename format
    pattern = r"(\d{8})_(\d)_([01])_(\w+)_(\w+)\.txt"
    
    # Use regex to extract components from the filename
    match = re.match(pattern, filename)
    
    if match:
        # Extract the components from the matched groups
        date = match.group(1)  # YYYYMMDD
        eid = int(match.group(2))  # Exercise ID (integer)
        position = int(match.group(3))  # Position (integer)
        name = match.group(4)  # Name
        daily_count = match.group(5)  # Daily Count
        
        # Map EID and Position to their full names using the provided dictionaries
        exercise = exercise_mapping.get(eid, "Unknown")
        position_name = position_mapping.get(position, "Unknown")
        
        # Create the resulting dictionary
        data = {
            "date": date,
            "exercise": exercise,
            "position": position_name,
            "name": name,
            "daily_count": daily_count
        }
        
        return data
    else:
        raise ValueError(f"Filename '{filename}' does not match expected format.")
