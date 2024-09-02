# Problem 3

### 1. Check Syntax Errors
- Parse the query to SQLParser to catch syntax errors.
- Verify the syntax by running the query on a mock database.

### 2. Validate Knowledge Graph
- Build a knowledge graph of the database schema and the generated SQL query.
- Ensure logical relationships are correct (e.g., "people work in finance companies and attend tech events," not "companies work at people").

### 3. Align with User Intent
- Use NLP to confirm that the query aligns with what the user intended.

### 4. Analyze Historical Queries
- Use a `failure_success_db` to find and compare similar past queries to give the llm an idea of how such a query was succesfully handled in the past.

### 5. Cross-Check with LLMs
- Validate the query by comparing results from multiple LLMs and choose the most voted candidate.

### 6. Iterate and Refine
- Provide the LLM with the findings of 1-5 to refine the query in the next iteration before providing it to the user.
