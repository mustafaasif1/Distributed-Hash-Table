# DistributedHashTable
This assignment was a part of the course Network-Centric Computing taught by Sir Zafar Ayub Qazi at Lahore University of Management Sciences

## Objective
In this assignment, we built a peer-peer file sharing system. The system was be a Distributed
Hash Table (DHT) based key-value storage system that has only two basic operations; put() and
get(). put() operation takes as input a filename, evaluates its key using some hash function, and
places the file on one of the nodes in the distributed system. Whereas, get() function takes as input a
filename, finds the appropriate node that can potentially have that file and returns the file if it exists
on that node. We will make this system failure tolerant i.e. it should not lose any data in case of
failure of a node.
I built the DHT using consistent hashing. Consistent hashing is a scheme for
distributing key-value pairs, such that distribution does not depend upon number of nodes in the
DHT. This allows to scale the size of DHT easily i.e. addition or removal of new nodes in the system
is not a costly operation.
