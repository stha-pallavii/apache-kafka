# schema of json files of transformed dataframes

# 1. Count the number of TV shows of each genre and sort from highest to lowest.
q1_genre_shows_count_schema = {
    "type": "struct",
    "optional": "false",
    "fields":[
        {
            "type": "string",
            "optional": "true",
            "field" : "genre"
        },
        {
            "type": "int64",
            "optional": "false",
            "field": "count"
        }
    ]
}

# 2. List the tv shows by type (Scripted/Reality/Animation, etc.) and status (running or ended), then give the number of shows in each list
q2_shows_list_count_schema = {
    "type": "struct",
    "optional": "false",
    "fields": [
        {
            "type": "string",
            "optional": "true",
            "field": "Type"
        },
        {
            "type": "string",
            "optional": "true",
            "field": "Status"  
        },
        {
            "type": "array",
            "optional": "false",
            "field": "TV Shows List",
            "items":{
            "type": "string"
            }
        },
        {
            "type": "int64",
            "optional": "true",
            "field": "Number of Shows"
        }
    ]
}


# 3. Find the average, maximum, and minimum weight of shows grouped by network name
q3_shows_weight_schema = {
    "type": "struct",
    "optional": "false",
    "fields": [
        {
            "type": "string",
            "optional": "true",
            "field": "Network Name"
        },
        {
            "type": "double",
            "optional": "true",
            "field": "Average Weight"  
        },
        {
            "type": "int64",
            "optional": "true",
            "field": "Maximum Weight"
        },
        {
            "type": "int64",
            "optional": "true",
            "field": "Minimum Weight"
        }
    ]
}



# 4. Highest rated TV show(s) of each country along with rating.
q4_highest_rated_shows_schema= {
    "type": "struct",
    "optional": "false",
    "fields": [
        {
            "type": "string",
            "optional": "true",
            "field": "Country"
        },  
        {
            "type": "float",
            "optional": "true",
            "field": "Rating"  
        },
        {
            "type": "array",
            "optional": "false",
            "field": "Highest Rated Show(s)",
            "items": {
            "type": "string"
        }
        }
    ]
}

