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

#ifndef GRAPHLAB_DISTRIBUTED_RANDOM_EC_REVERSE_INGRESS_HPP
#define GRAPHLAB_DISTRIBUTED_RANDOM_EC_REVERSE_INGRESS_HPP

#include <boost/functional/hash.hpp>

#include <graphlab/rpc/buffered_exchange.hpp>
#include <graphlab/graph/graph_basic_types.hpp>
#include <graphlab/graph/ingress/distributed_ingress_base.hpp>
#include <graphlab/graph/distributed_graph.hpp>


#include <graphlab/macros_def.hpp>
namespace graphlab {
  template<typename VertexData, typename EdgeData>
  class distributed_graph;

  /**
   * \brief Ingress object assigning vertices using randoming hash function.
   */
  template<typename VertexData, typename EdgeData>
  class distributed_random_ec_reverse_ingress : 
    public distributed_ingress_base<VertexData, EdgeData> {
  public:
    typedef distributed_graph<VertexData, EdgeData> graph_type;
    /// The type of the vertex data stored in the graph 
    typedef VertexData vertex_data_type;
    /// The type of the edge data stored in the graph 
    typedef EdgeData   edge_data_type;
    
    typedef distributed_ingress_base<VertexData, EdgeData> base_type;
    
    typedef typename base_type::edge_buffer_record edge_buffer_record;
    typedef typename base_type::vertex_buffer_record vertex_buffer_record;

   
  public:
    distributed_random_ec_reverse_ingress(distributed_control& dc, graph_type& graph) :
    base_type(dc, graph) {
    } // end of constructor

    ~distributed_random_ec_reverse_ingress() { }
    

    /** Add an edge to the ingress object using random assignment. */
    void add_vertex(vertex_id_type vid, std::vector<vertex_id_type>& adjacency_list,
                  const VertexData& vdata) {      
      const procid_t owning_proc = graph_hash::hash_vertex(vid) % base_type::rpc.numprocs();
      
      const vertex_buffer_record record(vid, vdata);      

      base_type::vertex_exchange.send(owning_proc, record, omp_get_thread_num());
      
      for(size_t i = 0 ; i < adjacency_list.size(); i++) {
          vertex_id_type target = adjacency_list[i];
          if(vid == target) { 
	      return;
	  }
	  const edge_buffer_record record(target, vid);
          base_type::edge_exchange.send(owning_proc, record, omp_get_thread_num() );
      }
      
    } // end of add vertex
  }; // end of distributed_random_ingress
}; // end of namespace graphlab
#include <graphlab/macros_undef.hpp>


#endif
