# Structure
* problem 1 solution consists of 1 file, problem_1/graphdb.py. Please run graphdb.py to see some sample filtering.
* problem 2 solution consists of 2 files, problem_2/generate_sample.py and problem_2/querygenerator.py. Please run the latter, which depends upon the former.
* problem 3 solution consists of problem_3/improve_sql.md

# Problem 1
## **Graph architecture & motivation**
We use a graph to store the database information in a way that relationships between entities can be quickly retrieved.
1. Each **Event**, **Company** and **Employee** to be stored as a **vertex** in a graph
2. To distinguish between the types of entities, the vertex class will have an attribute `vertex type`  attribute
3. Each dataframe will be parsed depending on **whether** the corresponding data is to be stored in a vertex or edge.
### Case 1: Info about just a single `vertex type` : **store in vertex**
Examples:
1.	All rows of `events_info` stored as attributes of `vert_type = event`
2.	All rows of `company_info` stored as attributes of `vert_type = company`

### Case 2: Info about relationship between distinct `vert_type` : **store in edge**
e.g.: 
#### Event Attendees
* edge between company and event of type `ATTEND`. Graph can be considered undirectional, since the relationship and its inverse are valid. More precisely, the relationship 
$$company\xrightarrow{\texttt{attend}}event$$
and its inverse:
$$event\xrightarrow{\texttt{attended<underscore>by}}company$$
are both implcitly represented by
$$company \xleftrightarrow{attend} event$$
and the respective columns are stored as attributes of the respective edge between the corresponding company and event.
<font color="red">Note:</font> this is a limitation of our current implementation. It is easy to modify the approach to allow for filtering  relationships like "person rated a company as 4/5" and "company rated the person as 5/5". Currently, the "hacky" way to do this is to represent person reviews & company reviews as 2 distinct edge labels



## Cases where such a representation helps
* When information needed like *"people who previously_attended a company in 2010 that is now attending an event in finance"*
	* A person might have worked at different companies for different time durations
## What works
1. Allows filtering as stated in problem 1: can receive as input a set of nodes and iteratively filter based on node conditions or edge conditions. This has been implemented and demonstrated in the python file.
2. (**not implemented** but possible by using the class functions) Allows iterative filtering based on subsets. For example, in case you want to run the query:
$$\texttt{people} -workingin-\texttt{financecompanies}-attending-\texttt{techevents}$$
can be run by popping each pair of `edge_condition - vertex_condition` starting from the end by filtering using the methods `filter_by_edge_conditions_for_set` and `filter_by_node_conditions_for_set`. 

**This is the kind of query where the graph based approach can demonstrate its peformance benefits because we consider only a subset of atomic-query results when processing the next steep.**  

## **Class & Functions Documentation**
We have the classes Node, Edge and GraphDB
## class Node
A node in the graph. 
### Attributes

- **node_id**: Unique identifier for the node. For current usage, we use company_url, event_url and people_id for this, as specified in the problem sheet.
- **node_type**: Type of the node. For current usage, it is restricted to 'people', 'company', or 'event', otherwise it throws an error.
- **attributes**: Attributes, i.e. in our usage, the columns of the dataframes
- **edges**: List of edges connected to this node. In our usage, it represents the relationships between entities

### Methods

* __init__(self, node_id, node_type)
* add_attribute(self, attribute, value)
* \_\_repr\_\_(self)
* add_edge(self, edge)

## class Edge
### Attributes

- **nodes**: Tuple of two Node objects connected by this edge. Note that we are storing node-edge relationships in both node and edge for faster lookup at the cost of increased memory. There is probably a better way to do this.
- **label**: Label of the edge.
- **attributes**: Dictionary storing edge attributes, i.e. in our case we will add dataframe column details when dataframes involve multiple columns

### Methods

* \_\_init\_\_(self, node1, node2, label)
* add_attribute(self, attribute, value)
* \_\_repr\_\_(self)

## class GraphDB
### Attributes

- **nodes**: Dictionary storing all nodes in the graph.

### Methods
* \_\_init\_\_(self)
*  add_node(self, node_id, node_type)
* add_edge(self, node1_id, node2_id, label)
* add_node_attribute(self, node_id, attribute, value)
* add_edge_attribute(self, node1_id, node2_id, label, attribute, value)
* filter_by_node_conditions_for_set(self, node_set, node_type, node_conditions) : Given the set `node_set`, it finds all nodes of particular type satisfying the condition belonging to the set 
* filter_by_edge_conditions_single_node(self, node_out_type, edge_label, edge_conditions, node_in_id) : given a single node, find all edges of particular label on that node satisfying the conditions 
* filter_by_edge_conditions_for_set(self, node_out_type, edge_label, edge_conditions, node_set) : set equivalent of above; find all nodes with edges satisfying the condition & label whose other node-end is in the given set
* **filter_node_set_global_filterer(self, filtering_node_type, filtering_node_attributes, nodes_set_in):** use for performing operations described in Problem (Node attributes filtering). Filters the input set `nodes_set_in` on the basis of node conditions specified, then also filters the related nodes based on graph relation
* **filter_edge_set_global_filterer(self, filtering_node_source_type, filtering_node_target_type, filtering_edge_label, filtering_edge_conditions, node_set_in):** use for performing operations described in Problem (Edge attributes filtering). Filters the input set `nodes_set_in` on the basis of source/destination types, edge label and edge condition, then also filters the node type not connected by the edges specified  based on graph relationship with the source & target type node
* **df_to_graph_insert_at_node(self, df, uid_column_name, node_type):** takes a python dataframe as input. If specified nodes don't exist, then it creates them with specified node_type. Finally, adds the attributes to the nodes identified by `uid_column_name`
* **df_to_graph_insert_as_edge(self, df, uid_column_node_origin, origin_type, uid_column_node_target, target_type, label)**: takes a python dataframe as input. If specified nodes don't exist, then it creates them. if specified edge between the nodes don't exist, then creates them as well. Finally, adds the attributes to the edge of given label joining nodes identified by `uid_column_node_origin` and `uid_column_name_target` 

## **Improvements Required**
* GraphDB class has become very large, needs to be split
* Order of arguments in functions is inconsistent, lead to confusion when using the functions
* Undirected edges are neither robust nor neat, should be replaced by directed edges. For same operation, can store by appending `_INVERSE` at the end of edge label to denote reverse, for example
* Error handling is haphazard and was implemented on need basis. Leads to haphazard behavior in case illegal operations are performed
* Create separate file for creating dataframes, and separate file for running everything
  
# Problem 2

Note: Old data is deleted every time the script runs and new data is added. Data comes from dynamic DataFrames stored in `generate_Sample.py`. I intentionally kept it this way for the ease of testing

## Overview
## Functions

### `build_query`

- This function creates SQL queries for specific tables.
- It unites filter arguments and output columns to avoid unnecessary pivoting.
- The basic structure of the `SELECT` statement is built first.
- Then, the logic for filtering with conditions—`includes`, `greater-than`, and `less-than`—is applied. This creates the `WHERE` clause.

### `query_data`

- This function groups conditions and output columns based on whether they belong to `event`, `company`, or `people`.
- After grouping, it builds three different queries.
- The queries run together using threading for optimization.


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
