# DS1_Project (DHT Node)

To run this application there are 3 options. 

##Starting a first disconnected node 
NodeApp conf_node1

##Join: bootstrap node using any other node in the network
NodeApp conf_node2 join ip_node1 port_node1

##Recover: bootstrap node using any other node in the network
NodeApp conf_node2 recover ip_node1 port_node1
