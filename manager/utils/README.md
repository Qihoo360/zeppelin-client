DprdWrapper Class Usage:

1. New DprdWrapper class

   ```c++
   DprdWrapper* dprd = new DprdWrapper;
   ```

2. Load buckets topology and rules

   ```c++
   std::string load_file = "dump_tree_test.example";
   dprd->LoadTree(load_file));
   ```

3. Generate partitions

   3.1 insert partitions

   ```c++
   // @param root_id: bucket id where starts this distribution
   // @param partition: partition id
   Distribute(int root_id, int partition);
   ```

   ​

   ```c++
   int partition_size = 1000;
   for (int i = 0; i < partition_size; ++i) {
     dprd->Distribute(0, i);
   }
   ```

   Function Distribute will distribute partition  [0, partition_size) in to buckets.

   3.2 load partitions

   ```c++
   bool DprdWrapper::LoadPartition(
       const std::map<int, std::vector<std::string>>& distribution); 
   ```

   ​

   ```c++
   std::map<int, std::vector<std::string> > partition_to_node;
   dprd->LoadPartition(partition_to_node);
   ```


4. Add/Remove bucket

   4.1 Add bucket

   4.1.1 Use AddBucket Function

   ```c++
   bool DprdWrapper::AddBucket(int type, int id, const std::string& name,
       int weight, int parent, const std::string& ip, int port);
   ```

   ​

   ```c++
   enum {
     kBucketTypeRoot,
     kBucketTypeRack,
     kBucketTypeHost,
     kBucketTypeNode
   };
   int node_id = 10;
   std::string node_name = "node10";
   int node_weight = 1;
   int node_parent_id = -10;
   std::string ip = "1.1.1.1";
   int port = 1111;
   // Add a node
   dprd->AddBucket(kBucketTypeNode, node_id, node_name, node_weight,
       node_parent_id, ip, port);
   int host_id = -20;
   std::string host_name = "host-20";
   int host_weight = 0;
   int host_parent_id = -15;
   // Add a host
   dprd->AddBucket(kBucketTypeHost, host_id, host_name, host_weight, host_parent_id);
   int rack_id = -3;
   int rack_name = "rack-3";
   int rack_weight = 0;
   int rack_parent = 0;
   // Add a rack
   dprd->AddBucket(kBucketTypeRack, rack_id, rack_name, rack_weight, rack_parent_id);
   int root_id = 0;
   int root_name = "root";
   int root_weight = 0;
   int root_parent_id = 0;
   // Add root
   dprd->AddBucket(kBucketTypeRoot, root_id, root_name, root_weight, root_parent_id);
   ```

   NOTICE

   1. Node ID is positive. Host and rack ID are negative. Root ID is 0.
   2. Node weight is 1. Host, rack and root weight are all initialized to be 0.
   3. When adding multiple buckets, please make sure current parent of adding bucket exist already.

   4.1.2 Modify Topology File

   1. Save current partition to nodes map by calling "BuildPartitionToNodesMap"
   2. Modify Topology File (Add new buckets' information)
   3. Reload topology and partitions

   4.2 Remove bucket

   ```c++
   bool DprdMap::RemoveBucket(int id);
   ```

   ​

   ```c++
   dprd->RemoveBucket(10); // Remove a node
   dprd->RemoveBucket(-10); // Remove a host or rack
   ```

5. Migrate

   ```c++
   dprd->Migrate();
   ```

   Do dprd process.

6. Build partition to nodes map

   ```c++
   bool DprdWrapper::BuildPartitionToNodesMap(
     std::map<int, std::vector<std::string> >* partition_to_nodes);
   ```

   ​

   ```c++
    std::map<int, std::vector<std::string> > partition_to_node;
      dprd->BuildPartitionToNodesMap(&partition_to_node);
   ```


​      

   partition_to_node stores data like

   partition: 1      nodes: 192.168.1.1:1111,   192.168.1.2:1112,   192.168.1.3:1113


