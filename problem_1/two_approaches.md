Two Solutions to the Problem

# 1. Graph-Based Database:
* Implemented as outlined in the README.
# 2. Entity-Based Graph Approach:
* Create three classes: Person, Event, and Company.
* Store node details within class attributes.
* Store relationships as edges with edge attributes.
* Query nodes through their respective classes. For interconnected queries, navigate through the classes and their relationships.
# Our Approach

I chose the first solution for these reasons:

* Graphs allow for easy updates to node structures when new attributes are added from additional data.
* Graph traversals efficiently handle complex queries. For example, finding people working in companies attending tech events, who previously worked for companies now attending pharmaceutical events, is optimized by traversing the relationships within the graph.
