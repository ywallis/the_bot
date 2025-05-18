import os

class QueryLoader:
    def __init__(self, query_dir="queries"):
        self.query_dir = query_dir
        self.queries = {}

    def load_queries(self):
        """Load all SQL queries from the query directory."""
        for root, _, files in os.walk(self.query_dir):
            for file in files:
                if file.endswith(".sql"):
                    query_name = os.path.splitext(file)[0]  # e.g., get_user_by_id
                    file_path = os.path.join(root, file)
                    with open(file_path, "r") as f:
                        self.queries[query_name] = f.read()

    def get_query(self, query_name):
        """Retrieve a query by its name."""
        return self.queries.get(query_name, None)
