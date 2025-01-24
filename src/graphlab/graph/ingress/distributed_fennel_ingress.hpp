/**  
 * Copyright (c) 2009 Carnegie Mellon University. 
 *     All rights reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS
 *  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied.  See the License for the specific language
 *  governing permissions and limitations under the License.
 *
 * For more about this software visit:
 *
 *      http://www.graphlab.ml.cmu.edu
 *
 */

#ifndef GRAPHLAB_DISTRIBUTED_FENNEL_INGRESS_HPP
#define GRAPHLAB_DISTRIBUTED_FENNEL_INGRESS_HPP

#include <boost/functional/hash.hpp>

#include <graphlab/logger/logger.hpp>
#include <graphlab/logger/assertions.hpp>

#include <graphlab/rpc/buffered_exchange.hpp>
#include <graphlab/graph/graph_basic_types.hpp>
#include <graphlab/graph/graph_hash.hpp>
#include <graphlab/graph/ingress/distributed_ingress_base.hpp>
#include <graphlab/graph/distributed_graph.hpp>

#include <graphlab/macros_def.hpp>

#include <limits>

namespace graphlab {
    template<typename VertexData, typename EdgeData>
    class distributed_graph;
   
    /**
     * \brief Ingress object assigning vertices using Fennel heurisic.
     */
    template<typename VertexData, typename EdgeData>
    class distributed_fennel_ingress :
    public distributed_ingress_base<VertexData, EdgeData> {
    public:
        typedef distributed_graph<VertexData, EdgeData> graph_type;
        /// The type of the vertex data stored in the graph 
        typedef VertexData vertex_data_type;
        /// The type of the edge data stored in the graph 
        typedef EdgeData edge_data_type;
        typedef typename graph_type::vertex_record vertex_record;

        typedef distributed_ingress_base<VertexData, EdgeData> base_type;

        typedef typename base_type::edge_buffer_record edge_buffer_record;
        typedef typename buffered_exchange<edge_buffer_record>::buffer_type 
            edge_buffer_type;
        typedef typename base_type::vertex_buffer_record vertex_buffer_record;
        typedef typename buffered_exchange<vertex_buffer_record>::buffer_type 
            vertex_buffer_type;
        
        typedef typename base_type::vertex_negotiator_record 
            vertex_negotiator_record;
        
        typedef typename base_type::vid2lvid_map_type vid2lvid_map_type;
        typedef typename base_type::vid2lvid_pair_type vid2lvid_pair_type;

        //Fennel specific data structures
        typedef typename boost::unordered_map<vertex_id_type, procid_t>
            placement_hash_table_type;
        typedef typename std::pair<vertex_id_type, procid_t>
            placement_pair_type;

        placement_hash_table_type dht_placement_table;
        std::vector<placement_pair_type> placement_buffer;
        rwlock dht_placement_table_lock;
        
	size_t PLACEMENT_BUFFER_THRESHOLD = 65536;       
 
        std::vector<size_t> partition_capacity;

        const size_t tot_nedges;
        const size_t tot_nverts;
        const size_t nprocs;
        procid_t self_pid;
        size_t capacity_constraint;

        dc_dist_object<distributed_fennel_ingress> fennel_rpc;
        
        // parameters for Fennel algorithm
        double alpha;
        double gamma;
	double balance_slack = 0.05;  
        
        // if there is only one loader, communication with other loaders are disabled
        bool single_loader;
        // use edge balanced strategy instead of vertex balanced
        bool edge_balanced;

    public:

        distributed_fennel_ingress(distributed_control& dc, graph_type& graph,
                size_t tot_nedges = 0, size_t tot_nverts = 0, bool edge_balanced = true, bool single_loader = false) :
                base_type(dc, graph),  
                fennel_rpc(dc, this), nprocs(dc.numprocs()), tot_nedges(tot_nedges), tot_nverts(tot_nverts), 
                partition_capacity(dc.numprocs(), 0), single_loader(single_loader), edge_balanced(edge_balanced) {
            
            self_pid = fennel_rpc.procid();

            // fennel specific parameters
            gamma = 1.5;
            alpha = sqrt(nprocs) * double(tot_nedges) / pow(tot_nverts, gamma);
            
            if(edge_balanced) {
                capacity_constraint = (tot_nedges / nprocs) * (1 + balance_slack);
            } else {
                capacity_constraint = (tot_nverts / nprocs) * (1 + balance_slack);
            }
            
            logstream(LOG_INFO) << "Fennel Ingress edge balanced: " << edge_balanced << " single loader (no sync): " << single_loader << std::endl;
        } // end of constructor

        ~distributed_fennel_ingress() {
        }

        /** Add an edge to the ingress object using random assignment. */
        void add_vertex(vertex_id_type vid, std::vector<vertex_id_type>& adjacency_list,
                const VertexData& vdata) {
            // initialize all neighbour counts with 0
            std::vector<float> neighbour_count(nprocs, 0);
            std::vector<float> candidate_partitions;
            
            // query partition id of each neighbour and count neighbours in each partition
            for (size_t i = 0; i < adjacency_list.size(); i++) {
                procid_t neighbour_owner = get_vertex_partition(adjacency_list[i]);
                if (neighbour_owner !=  ((procid_t)-1)) {
                    neighbour_count[neighbour_owner]++;
                }
            }
 		
            float best_score = -std::numeric_limits<float>::max();
            
            for (size_t i = 0; i < nprocs; i++) {
                // get current capacity for partition i
                size_t current_partition_capacity = partition_capacity[i];
                    
                if(current_partition_capacity > capacity_constraint) {
                    // do not consider this partition
                    continue;
                }
                
                // compute partition i score
                float partition_score = 0;
                partition_score = neighbour_count[i] - (alpha * gamma * pow(current_partition_capacity, (gamma - 1)) );
                if(partition_score > best_score) {
                    candidate_partitions.clear();
                    best_score = partition_score;
                    candidate_partitions.push_back(i); 
                } else if(partition_score == best_score) {
                    candidate_partitions.push_back(i);
                }
            }

            //choose partition randomly from the candidate partitions
            const procid_t owning_proc = candidate_partitions[graph_hash::hash_vertex(vid) % candidate_partitions.size()];
            set_vertex_partition(vid, adjacency_list, owning_proc);

            const vertex_buffer_record record(vid, vdata);

#ifdef _OPENMP
            base_type::vertex_exchange.send(owning_proc, record, omp_get_thread_num());
#else
            base_type::vertex_exchange.send(owning_proc, record);
#endif

            for (size_t i = 0; i < adjacency_list.size(); i++) {
                vertex_id_type target = adjacency_list[i];
                if (vid == target) {
                    continue;
                }
                const edge_buffer_record record(vid, target);
#ifdef _OPENMP
                base_type::edge_exchange.send(owning_proc, record, omp_get_thread_num());
#else
                base_type::edge_exchange.send(owning_proc, record);
#endif
            }

        } // end of add vertex
        
        void finalize() {
            // communicate for the remaining part of placement_buffer
            // then call finalize from base class
            if(!placement_buffer.empty()) {
                for(size_t i = 0 ; i < nprocs ; i++) {
                    // only populate the the ones that do not belong to this process
                    if(i != self_pid) {
                        // need remote call to populate dht
                        fennel_rpc.remote_request(i, &distributed_fennel_ingress::block_add_placement_pair, self_pid, placement_buffer);
                    } 
                }
                placement_buffer.clear();
            }
            
            //call base types finalize method
            base_type::finalize();
        }
        
    protected:
        virtual void determine_master(vid2lvid_map_type& vid2lvid_buffer) {
           
        /**************************************************************************/
      /*                                                                        */
      /*        assign vertex data and allocate vertex (meta)data  space        */
      /*                                                                        */
      /**************************************************************************/
            std::cout << "FENNEL DETERMINE MASTER" << std::endl;
       // Determine masters for all negotiated vertices
        const size_t local_nverts = base_type::graph.vid2lvid.size() + vid2lvid_buffer.size();
        base_type::graph.lvid2record.reserve(local_nverts);
        base_type::graph.lvid2record.resize(local_nverts);
        base_type::graph.local_graph.resize(local_nverts);
        foreach(const vid2lvid_pair_type& pair, vid2lvid_buffer) {
            vertex_record& vrec = base_type::graph.lvid2record[pair.second];
            vrec.gvid = pair.first;
            vrec.owner = dht_placement_table[pair.first];
        }
        ASSERT_EQ(local_nverts, base_type::graph.local_graph.num_vertices());
        ASSERT_EQ(base_type::graph.lvid2record.size(), base_type::graph.local_graph.num_vertices());
        if(fennel_rpc.procid() == 0)       
          memory_info::log_usage("Finihsed allocating lvid2record");
      }
        
    private:
            /**
         * Acquires read lock on the distributed table and returns partition procid for given vertex
         * @param vid
         * @return -1 if entry does not exist
         */
        procid_t get_vertex_partition(vertex_id_type vid) {
            procid_t partition;
            // TODO: removed the read_lock as knowing exact partition does not have significant impact in performance    
            if (dht_placement_table.find(vid) == dht_placement_table.end()) {
                partition = -1;
            } else {
                partition = dht_placement_table[vid];
            }
            return partition;
        }

        /**
         * Acquires write lock on the table and populated the partition entry for given vertex
         * @param vid
         * @param procid
         */
        void set_vertex_partition(vertex_id_type vid, std::vector<vertex_id_type>& adjacency_list, procid_t procid) {
            dht_placement_table_lock.writelock();
            dht_placement_table[vid] = procid;
            if(!single_loader)
                placement_buffer.push_back(placement_pair_type(vid, procid));
            
            // increase local partition capacity
            if(edge_balanced)
                partition_capacity[procid] += adjacency_list.size();
            else
                partition_capacity[procid]++;
            
            dht_placement_table_lock.wrunlock();
            
            // std::cout << "Vertex:" << vid << "  Partition:" << procid << std::endl;
            
            // check whether we need to sync blocks
            if(placement_buffer.size() > PLACEMENT_BUFFER_THRESHOLD) {
                for(size_t i = 0 ; i < nprocs ; i++) {
                    // only populate the the ones that do not belong to this process
                    if(i != self_pid) {
                        // need remote call to populate dht
                        // use unblocking calls for single loader case, so that loader process can continue
                        fennel_rpc.remote_request(i, &distributed_fennel_ingress::block_add_placement_pair, self_pid, placement_buffer);
                    }
                }
                placement_buffer.clear();
            }
        }
        
        void block_add_placement_pair(procid_t pid, std::vector<placement_pair_type>& placement_buffer) {
            dht_placement_table_lock.writelock();
            
            foreach( placement_pair_type& placement, placement_buffer ) {
                dht_placement_table[placement.first] = placement.second;
                // update partition capacity
                partition_capacity[pid]++;
                
                // std::cout << "From " << pid << " to " << this->self_pid << " assignment" << placement.first << " : " << placement.second << std::endl;
            }
            
            dht_placement_table_lock.wrunlock();
        }

    }; // end of distributed_fennel_ingress
}; // end of namespace graphlab
#include <graphlab/macros_undef.hpp>


#endif
