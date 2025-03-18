import requests
import pandas as pd
import time

# Define the correct GraphQL API endpoint
GRAPHQL_ENDPOINT = "https://clubs-api.raspberrypi.org/graphql"

# Authorization header
HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "[TOKEN]"
}

# Define the GraphQL query
GRAPHQL_QUERY = """
query GetClubs($partnershipUuid: String, $status: [ClubStatus!], $clubName: String, $first: Int, $after: String) {
  clubs(
    first: $first
    after: $after
    filterBy: {name: $clubName, status: $status, partnershipUuid: $partnershipUuid}
  ) {
    nodes {
      id
      uuid
      name
      lookingForVolunteers
      municipality
      venueType
      status
      createdAt
      updatedAt
      roles(first: 1, filterBy: {userRole: [CHAMPION, ORGANISER]}) {
        nodes {
          user {
            name
          }
        }
      }
    }
    pageInfo {
      endCursor
      hasNextPage
    }
  }
}
"""

def fetch_all_clubs():
    """Fetch all clubs while handling pagination properly."""
    all_clubs = []
    after_cursor = None

    while True:
        query_variables = {
            "partnershipUuid": "d0605126-8180-4c15-acb7-8c55ad76ffb0",
            "status": [
                "APPLICATION_IN_PROGRESS",
                "CLOSED",
                "PLANNING",
                "READY_FOR_VERIFICATION",
                "RUNNING_SESSIONS"
            ],
            "clubName": "",
            "first": 50,
            "after": after_cursor
        }

        try:
            response = requests.post(
                GRAPHQL_ENDPOINT, 
                json={"query": GRAPHQL_QUERY, "variables": query_variables},
                headers=HEADERS
            )

            if response.status_code != 200:
                raise Exception(f"Query failed: {response.status_code}, {response.text}")

            data = response.json()

            # Extract relevant parts of the response
            clubs = data.get("data", {}).get("clubs", {}).get("nodes", [])
            page_info = data.get("data", {}).get("clubs", {}).get("pageInfo", {})

            if not clubs:
                print("No clubs found or no more data.")
                break

            all_clubs.extend(clubs)
            print(f"Fetched {len(clubs)} clubs. Total collected: {len(all_clubs)}.")

            # Check if there are more pages
            if page_info.get("hasNextPage"):
                after_cursor = page_info.get("endCursor")  # Update cursor
                time.sleep(1)  # Rate-limit friendly delay
            else:
                break  # Stop when no more pages

        except Exception as e:
            print(f"Error: {e}")
            break

    return all_clubs

def save_to_csv(clubs, filename="clubs_data.csv"):
    """Save club data to a CSV file, handling nested fields."""
    if not clubs:
        print("No data to save.")
        return

    df = pd.DataFrame(clubs)

    # Extract Champion/Organiser names from nested structure
    df["champion_or_organiser"] = df["roles"].apply(
        lambda x: x["nodes"][0]["user"]["name"] if x["nodes"] else None
    )

    # Drop the nested column
    df.drop(columns=["roles"], inplace=True)

    # Save to CSV
    df.to_csv(filename, index=False)
    print(f"✅ Data saved to {filename}")

if __name__ == "__main__":
    clubs_data = fetch_all_clubs()
    save_to_csv(clubs_data)
