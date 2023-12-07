# Azure function for Streaming Data to Timescale DB



# Testing "localy" with github codespaces
Copy the path after opening the running azure function on another tab.

For postman requests you need the github token. Open a terminal in visual studio code in codespaces and type:

    echo $GITHUB_TOKEN 

copy the token and add it in your postman request as a Header with key= "X-GITHUB-TOKEN" and value="the copied token"