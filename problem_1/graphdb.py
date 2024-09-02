import pandas as pd
from tabulate import tabulate

class Node:
    def __init__(self, node_id, node_type):
        if node_type not in ['people', 'company', 'event']:
            raise ValueError("Node type must be 'people', 'company', or 'event'")
        self.node_id = node_id
        self.node_type = node_type
        self.attributes = {}
        self.edges = []  # Store edges locally

    def add_attribute(self, attribute, value):
        self.attributes[attribute] = value

    def add_edge(self, edge):
        self.edges.append(edge)

    def __repr__(self):
        return f"Node({self.node_id}, {self.node_type}, {self.attributes})"


class Edge:
    def __init__(self, node1, node2, label):
        self.nodes = (node1, node2)
        self.label = label
        self.attributes = {}

    def add_attribute(self, attribute, value):
        self.attributes[attribute] = value

    def __repr__(self):
        return f"Edge({self.nodes[0].node_id} --[{self.label}]-- {self.nodes[1].node_id}, {self.attributes})"


class GraphDB:
    def __init__(self):
        self.nodes = {}

    def add_node(self, node_id, node_type):
        if node_id in self.nodes:
            raise ValueError("Node ID already exists")
        node = Node(node_id, node_type)
        self.nodes[node_id] = node
        return node

    # storing edge with each node. TODO: should be more robust and directional. works currently since reverse direction edges/self edges dont exist
    def add_edge(self, node1_id, node2_id, label):
        if node1_id not in self.nodes or node2_id not in self.nodes:
            raise ValueError("Both nodes must exist in the graph")
        node1 = self.nodes[node1_id]
        node2 = self.nodes[node2_id]
        edge = Edge(node1, node2, label)
        node1.add_edge(edge)
        node2.add_edge(edge)
        return edge

    def add_node_attribute(self, node_id, attribute, value):
        if node_id not in self.nodes:
            raise ValueError("Node does not exist")
        self.nodes[node_id].add_attribute(attribute, value)

    def add_edge_attribute(self, node1_id, node2_id, label, attribute, value):
        node1 = self.nodes[node1_id]
        for edge in node1.edges:
            if (edge.nodes[0].node_id == node1_id and edge.nodes[1].node_id == node2_id and edge.label == label) or \
               (edge.nodes[0].node_id == node2_id and edge.nodes[1].node_id == node1_id and edge.label == label):
                edge.add_attribute(attribute, value)
                return
        raise ValueError("Edge does not exist")

    def filter_by_node_conditions_for_set(self, node_set, node_type, node_conditions):
        result_set = []
        if not node_conditions:
            return node_set

        for node_id in node_set:
            if node_id not in self.nodes:
                raise ValueError(f"Node {node_id} does not exist")

            node_in = self.nodes[node_id]
            meets_conditions = True
            if node_in.node_type != node_type:
                meets_conditions = False

            for condition, value in node_conditions.items():
                if condition not in node_in.attributes:
                    meets_conditions = False
                    break
                if node_in.attributes[condition] != value:
                    meets_conditions = False
                    break

            if meets_conditions:
                result_set.append(node_in)

        return result_set

    # for a SINGLE node_in, return: 
    #   all nodes of a type
    #   connecting to node_in via edges with label and conditions specified
    def filter_by_edge_conditions_single_node(self, node_out_type, edge_label, edge_conditions, node_in_id):
        if node_in_id not in self.nodes:
            raise ValueError("Node does not exist")

        node_in = self.nodes[node_in_id]
        result = []

        for edge in node_in.edges:
            # if no edge conditions then need to only check for correct label
            if edge.label == edge_label and not edge_conditions:
                for node in edge.nodes:
                    # need to do a "node_id != node_in_id" because each edge is stored in both nodes, so self will always point to self otherwise
                    if node.node_type == node_out_type and node.node_id != node_in_id:
                        result.append(node)
                continue
 

            if edge.label == edge_label and all(edge.attributes.get(k) == v for k, v in edge_conditions.items()):
                for node in edge.nodes:
                    # need to do a "node_id != node_in_id" because each edge is stored in both nodes, so self will always point to self otherwise
                    if node.node_type == node_out_type and node.node_id != node_in_id:
                        result.append(node)

        return result

    def filter_by_edge_conditions_for_set(self, node_out_type, edge_label, edge_conditions, node_set):
        result_set = set()

        for node_id in node_set:
            if node_id not in self.nodes:
                raise ValueError(f"Node {node_id} does not exist")

            filtered_nodes = self.filter_by_edge_conditions_single_node(node_out_type, edge_label, edge_conditions, node_id)
            for node in filtered_nodes:
                result_set.add(node)

        return list(result_set)   

    def filter_node_set_global_filterer(self, filtering_node_type, filtering_node_attributes, nodes_set_in):
        result_set = []

        # add nodes of original type filtering_node_type by filtering simply on node attributes:
        nodes_original_type = graph.filter_by_node_conditions_for_set(nodes_set_in, filtering_node_type, filtering_node_attributes)
        nodes_original_type = set(nodes_original_type)

        nodes_original_type_ids = {node.node_id for node in nodes_original_type}  

        # add nodes for other types by edge based filtering
        nodes_new_type = {}
        empty_conditions = set()

        # case: event => find all companies attending the subset of events && find all people working in those companies
        if filtering_node_type == 'event':
            nodes_new_type_company = graph.filter_by_edge_conditions_for_set('company', 'attends', empty_conditions, nodes_original_type_ids)
            nodes_new_type_company = set(nodes_new_type_company)
            nodes_new_type_company_ids = {node.node_id for node in nodes_new_type_company}  

            nodes_new_type_people  = graph.filter_by_edge_conditions_for_set('people', 'works_at', empty_conditions, nodes_new_type_company_ids)
            nodes_new_type_people  = set(nodes_new_type_people)

            nodes_new_type = nodes_new_type_company.union(nodes_new_type_people)

        # case: people => find all companies people work at && find all events those companies attending
        elif filtering_node_type == 'people':
            nodes_new_type_company = graph.filter_by_edge_conditions_for_set('company', 'works_at', empty_conditions, nodes_original_type_ids)
            nodes_new_type_company = set(nodes_new_type_company)
            nodes_new_type_company_ids = {node.node_id for node in nodes_new_type_company}  

            nodes_new_type_event = graph.filter_by_edge_conditions_for_set('event', 'attends', empty_conditions, nodes_new_type_company_ids)
            nodes_new_type_event  = set(nodes_new_type_event)

            nodes_new_type = nodes_new_type_company.union(nodes_new_type_event)

        #case: company => find all people working there and all events it is attending
        elif filtering_node_type == 'company':
            nodes_new_type_people = graph.filter_by_edge_conditions_for_set('people', 'works_at', empty_conditions, nodes_original_type_ids)
            nodes_new_type_people  = set(nodes_new_type_people)
            nodes_new_type_event = graph.filter_by_edge_conditions_for_set('event', 'attends',  empty_conditions, nodes_original_type_ids)
            nodes_new_type_event  = set(nodes_new_type_event)

            nodes_new_type = nodes_new_type_people.union(nodes_new_type_event)

        else:
            raise ValueError(f"Unknown filtering_node_type: {filtering_node_type}")

        result_set = nodes_original_type | nodes_new_type

        result_filtered = []
        for node in result_set:
            if node.node_id in nodes_set_in:
                result_filtered.append(node)
        return result_filtered

    def filter_edge_set_global_filterer(self, filtering_node_source_type, filtering_node_target_type, filtering_edge_label, filtering_edge_conditions, node_set_in):
        filtering_node_combined_type = {filtering_node_source_type, filtering_node_target_type}
        if 'company' not in filtering_node_combined_type:
            raise ValueError(f"can not filter edges between people and events for the current schema")
        
        elif 'people' not in filtering_node_combined_type:
            filtering_node_remaining_type = 'people'
            remaining_edge_label = 'works_at'
        
        elif 'event' not in filtering_node_combined_type:
            filtering_node_remaining_type = 'event'
            remaining_edge_label = 'attends'
        
        else:
            raise ValueError(f"illegal type")
            

        # nodes of src type with edge drawing to set
        filtered_nodes_src_pre = graph.filter_by_edge_conditions_for_set(filtering_node_source_type, filtering_edge_label, filtering_edge_conditions, node_set_in)
        filtered_nodes_src = []
        for node in filtered_nodes_src_pre:
            if node.node_id in node_set_in:
                filtered_nodes_src.append(node)
        filtered_nodes_src = set(filtered_nodes_src)

        # nodes of tgt type with edge drawing to set
        filtered_nodes_tgt_pre = graph.filter_by_edge_conditions_for_set(filtering_node_target_type, filtering_edge_label, filtering_edge_conditions, node_set_in)
        filtered_nodes_tgt = []
        for node in filtered_nodes_tgt_pre:
            if node.node_id in node_set_in:
                filtered_nodes_tgt.append(node)
        filtered_nodes_tgt = set(filtered_nodes_tgt)
        filtered_nodes_total = filtered_nodes_src.union(filtered_nodes_tgt)
        filtered_nodes_total_id = {node.node_id for node in filtered_nodes_total}

        # remaining nodes
        empty_conditions = {}
        filtered_nodes_remaining = graph.filter_by_edge_conditions_for_set(filtering_node_remaining_type,remaining_edge_label, empty_conditions, filtered_nodes_total_id)
        filtered_nodes_remaining = set(filtered_nodes_remaining)

        filtered_nodes_total = filtered_nodes_total.union(filtered_nodes_remaining)
        return filtered_nodes_total

    def df_to_graph_insert_at_node(self, df, uid_column_name, node_type):
        for uid in df[uid_column_name].unique():
            # if node not exist, create it
            if uid not in self.nodes:
                self.add_node(uid, node_type)

            subset = df[df[uid_column_name] == uid]

            for _, row in subset.iterrows():
                for attribute, value in row.items():
                    if attribute != uid_column_name:
                        self.add_node_attribute(uid, attribute, value)

    def df_to_graph_insert_as_edge(self, df, uid_column_node_origin, origin_type, uid_column_node_target, target_type, label):
        for _, row in df.iterrows():
            origin_id = row[uid_column_node_origin]
            target_id = row[uid_column_node_target]

            # Create nodes if don't exist
            if origin_id not in self.nodes:
                self.add_node(origin_id, origin_type)
            if target_id not in self.nodes:
                self.add_node(target_id, target_type)

            # Create edge if don't exist
            try:
                self.add_edge(origin_id, target_id, label)
            except ValueError:
                pass

            # Add attributes to the edge
            for attribute, value in row.items():
                if attribute not in [uid_column_node_origin, uid_column_node_target]:
                    try:
                        self.add_edge_attribute(origin_id, target_id, label, attribute, value)
                    except ValueError:
                        pass

    def __repr__(self):
        return f"GraphDB(Nodes: {list(self.nodes.values())})"


graph = GraphDB()
# Events dataframe
events_data = {
    'event_url': ['event1.com', 'event2.com', 'event3.com', 'event4.com', 'event5.com'],
    'event_name': ['Tech Summit 2024', 'Green Energy Expo', 'Global Finance Forum', 'AI Revolution Conference', 'Healthcare Innovation Summit'],
    'event_start_date': ['2024-10-15', '2024-11-22', '2024-09-05', '2024-12-01', '2024-08-18'],
    'event_city': ['San Francisco', 'Berlin', 'New York', 'Tokyo', 'London'],
    'event_country': ['USA', 'Germany', 'USA', 'Japan', 'UK'],
    'event_industry': ['Technology', 'Energy', 'Finance', 'Technology', 'Healthcare']
}

df_events = pd.DataFrame(events_data)
graph.df_to_graph_insert_at_node(df_events, 'event_url', 'event')

# Companies dataframe
companies_data = {
    'company_url': ['techco.com', 'greenergy.com', 'megabank.com', 'aiinnovate.com', 'healthtech.com'],
    'company_name': ['TechCo', 'GreenErgy', 'MegaBank', 'AI Innovate', 'HealthTech'],
    'company_industry': ['Technology', 'Energy', 'Finance', 'Technology', 'Healthcare'],
    'company_revenue': ['$500M', '$200M', '$2B', '$100M', '$300M'],
    'company_country': ['USA', 'Germany', 'USA', 'Japan', 'UK']
}
df_companies = pd.DataFrame(companies_data)
graph.df_to_graph_insert_at_node(df_companies, 'company_url', 'company')

# Event attendees dataframe
attendees_data = {
    'event_url': ['event1.com', 'event1.com', 'event2.com', 'event3.com', 'event4.com', 'event5.com', 'event5.com'],
    'company_url': ['techco.com', 'aiinnovate.com', 'greenergy.com', 'megabank.com', 'aiinnovate.com', 'healthtech.com', 'techco.com'],
    'company_relation_to_event': ['Sponsor', 'Attendee', 'Exhibitor', 'Sponsor', 'Keynote Speaker', 'Sponsor', 'Attendee']
}

df_attendees = pd.DataFrame(attendees_data)
graph.df_to_graph_insert_as_edge(df_attendees, 'company_url', 'company', 'event_url', 'event', 'attends')

# Company contact info dataframe
contact_info_data = {
    'company_url': ['techco.com', 'greenergy.com', 'megabank.com', 'aiinnovate.com', 'healthtech.com'],
    'office_city': ['San Francisco', 'Berlin', 'New York', 'Tokyo', 'London'],
    'office_country': ['USA', 'Germany', 'USA', 'Japan', 'UK'],
    'office_address': ['123 Tech St', '456 Green Ave', '789 Finance Blvd', '101 AI Road', '202 Health Lane'],
    'office_email': ['info@techco.com', 'contact@greenergy.com', 'support@megabank.com', 'hello@aiinnovate.com', 'info@healthtech.com']
}
df_contact_info = pd.DataFrame(contact_info_data)
graph.df_to_graph_insert_at_node(df_contact_info, 'company_url', 'company')

# Company employee info dataframe
employee_data = {
    'company_url': ['techco.com'] * 4 + ['greenergy.com'] * 4 + ['megabank.com'] * 4 + ['aiinnovate.com'] * 4 + ['healthtech.com'] * 4,
    'person_id': range(1, 21),
    'person_first_name': ['John', 'Emma', 'Michael', 'Sophia', 'Lars', 'Greta', 'Hans', 'Ingrid', 'David', 'Sarah', 'Robert', 'Jennifer', 'Takashi', 'Yuki', 'Hiroshi', 'Aiko', 'James', 'Elizabeth', 'William', 'Olivia'],
    'person_last_name': ['Smith', 'Johnson', 'Brown', 'Davis', 'Schmidt', 'Muller', 'Weber', 'Fischer', 'Wilson', 'Taylor', 'Anderson', 'Thomas', 'Tanaka', 'Sato', 'Suzuki', 'Watanabe', 'Jones', 'White', 'Harris', 'Martin'],
    'person_email': [f"{fname.lower()}.{lname.lower()}@{company}" for fname, lname, company in zip(['John', 'Emma', 'Michael', 'Sophia', 'Lars', 'Greta', 'Hans', 'Ingrid', 'David', 'Sarah', 'Robert', 'Jennifer', 'Takashi', 'Yuki', 'Hiroshi', 'Aiko', 'James', 'Elizabeth', 'William', 'Olivia'], 
                                                                                                  ['Smith', 'Johnson', 'Brown', 'Davis', 'Schmidt', 'Muller', 'Weber', 'Fischer', 'Wilson', 'Taylor', 'Anderson', 'Thomas', 'Tanaka', 'Sato', 'Suzuki', 'Watanabe', 'Jones', 'White', 'Harris', 'Martin'],
                                                                                                  ['techco.com'] * 4 + ['greenergy.com'] * 4 + ['megabank.com'] * 4 + ['aiinnovate.com'] * 4 + ['healthtech.com'] * 4)],
    'person_city': ['San Francisco', 'San Jose', 'Oakland', 'Palo Alto', 'Berlin', 'Hamburg', 'Munich', 'Frankfurt', 'New York', 'Boston', 'Chicago', 'Los Angeles', 'Tokyo', 'Osaka', 'Kyoto', 'Yokohama', 'London', 'Manchester', 'Birmingham', 'Liverpool'],
    'person_country': ['USA'] * 4 + ['Germany'] * 4 + ['USA'] * 4 + ['Japan'] * 4 + ['UK'] * 4,
    'person_seniority': ['Senior'] * 5 + ['Mid-level'] * 10 + ['Junior'] * 5,
    'person_department': ['Engineering', 'Marketing', 'Sales', 'HR', 'Operations'] * 4
}
df_employees = pd.DataFrame(employee_data)
graph.df_to_graph_insert_as_edge(df_employees, 'person_id', 'people', 'company_url', 'company', 'works_at')

def display_table(df, title):
    print(f"\n{title}")
    print(tabulate(df, headers='keys', tablefmt='pretty', showindex=False))

display_table(df_events, "Events Dataframe")
display_table(df_companies, "Companies Dataframe")
display_table(df_attendees, "Event Attendees Dataframe")
display_table(df_contact_info, "Company Contact Info Dataframe")
display_table(df_employees, "Company Employee Info Dataframe")

event_set   = {'event1.com', 'event2.com', 'event3.com', 'event4.com', 'event5.com'}
company_set =  {'techco.com', 'greenergy.com', 'megabank.com', 'aiinnovate.com', 'healthtech.com'}
people_set  = set(range(1, 21))

full_set = event_set.union(company_set, people_set)
node_set = full_set

node_conditions = {'event_country':'USA'}
filtered_nodes_events_in_usa = graph.filter_node_set_global_filterer('event', node_conditions, node_set)
filtered_nodes_events_in_usa_idfied = {node.node_id for node in filtered_nodes_events_in_usa}  

node_conditions = {'company_name':'TechCo'}
filtered_nodes_events_in_usa_company_techco = graph.filter_node_set_global_filterer('company', node_conditions, filtered_nodes_events_in_usa_idfied)

print ("----------------------------------------------------------------------------------------------------------")
print ("----------------------------------------------------------------------------------------------------------")

print("\n(test node filtering) events in USA :\n")
for node in filtered_nodes_events_in_usa:
    print(node)

print ("----------------------------------------------------------------------------------------------------------")
print ("----------------------------------------------------------------------------------------------------------")


print("\n(test node filtering) events in USA. company is techco:\n")
for node in filtered_nodes_events_in_usa_company_techco:
    print(node)

print ("----------------------------------------------------------------------------------------------------------")
print ("----------------------------------------------------------------------------------------------------------")

node_conditions = {'company_relation_to_event':'Sponsor'}
node_set = full_set
filtered_nodes_sponsor_companies = graph.filter_edge_set_global_filterer('company','event','attends',node_conditions,full_set)
print ("\n filter companies that are sponsors: \n")
for node in filtered_nodes_sponsor_companies:
    print(node)

print ("----------------------------------------------------------------------------------------------------------")
print ("----------------------------------------------------------------------------------------------------------")

node_conditions = {'person_department':'Engineering'}
node_set = {node.node_id for node in filtered_nodes_sponsor_companies}
filtered_nodes_sponsor_companies_engineering_people = graph.filter_edge_set_global_filterer('people','company','works_at',node_conditions,node_set)
print ("\n filter companies that are sponsors and filter people in engineering: \n")
for node in filtered_nodes_sponsor_companies_engineering_people:
    print(node)

print ("----------------------------------------------------------------------------------------------------------")
print ("----------------------------------------------------------------------------------------------------------")
