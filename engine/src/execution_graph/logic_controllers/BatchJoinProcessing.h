#pragma once

#include "BatchProcessing.h"
#include "TaskFlowProcessor.h"
#include "BlazingColumn.h"
#include "LogicPrimitives.h"
#include "CacheMachine.h"
#include "TaskFlowProcessor.h"
#include "io/Schema.h"
#include "utilities/CommonOperations.h"
#include "communication/CommunicationData.h"
#include "parser/expression_utils.hpp"
#include "execution_graph/logic_controllers/LogicalFilter.h"
#include "distribution/primitives.h"

#include <cudf/hashing.hpp>
#include <cudf/join.hpp>

namespace ral {
namespace batch {
using ral::cache::kstatus;
using ral::cache::kernel;
using ral::cache::kernel_type;

const std::string INNER_JOIN = "inner";
const std::string LEFT_JOIN = "left";
const std::string RIGHT_JOIN = "right";
const std::string OUTER_JOIN = "full";

class PartwiseJoin : public PhysicalPlan {
public:
	PartwiseJoin(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context} {
		this->input_.add_port("input_a", "input_b");

		SET_SIZE_THRESHOLD = 300000000;
		this->max_left_ind = -1;
		this->max_right_ind = -1;
	}

	std::unique_ptr<ral::frame::BlazingTable> load_set(BatchSequence & input, bool load_all){
		
		std::size_t bytes_loaded = 0;
		std::vector<std::unique_ptr<ral::frame::BlazingTable>> tables_loaded;
		// WSM has_next should be a next_exists()
		while (input.has_next() && bytes_loaded < SET_SIZE_THRESHOLD) {
			tables_loaded.emplace_back(input.next());
			bytes_loaded ++ tables_loaded.back()->sizeInBytes();
		}
		if (tables_loaded.size() == 0){
			return nullptr;
		} else if (tables_loaded.size() == 1){
			return std::move(tables_loaded[0]);
		} else {
			std::vector<ral::frame::BlazingTableView> tables_to_concat(tables_loaded.size());
			for (std::size_t i = 0; i < tables_loaded.size(); i++){
				tables_to_concat[i] = tables_loaded[i]->toBlazingTableView();
			}
			return ral::utilities::experimental::concatTables(tables_to_concat);			
		}
	}

	std::unique_ptr<ral::frame::BlazingTable> load_left_set(){
		this->max_left_ind++;
		// need to load all the left side, only if doing a FULL OUTER JOIN
		bool load_all = this->join_type == OUTER_JOIN ?  true : false;
		return load_set(this->left_sequence, load_all);
	}

	std::unique_ptr<ral::frame::BlazingTable> load_right_set(){
		this->max_right_ind++;
		// need to load all the right side, if doing any type of outer join
		bool load_all = this->join_type != INNER_JOIN ?  true : false;
		return load_set(this->right_sequence, load_all);
	}

	void mark_set_completed(int left_ind, int right_ind){
		if (completion_matrix.size() <= left_ind){
			completion_matrix.resize(left_ind + 1);
		}
		if (completion_matrix[left_ind].size() <= right_ind){
			completion_matrix[left_ind].resize(right_ind + 1);
		}
		completion_matrix[left_ind][right_ind] = true;
	}

	// This function checks to see if there is a set from our current completion_matix (data we have already loaded once)
	// that we have not completed that uses one of our current indices, otherwise it returns [-1, -1]
	std::tuple<int, int> check_for_another_set_to_do_with_data_we_already_have(int left_ind, int right_ind){
		if (completion_matrix.size() > left_ind && completion_matrix[left_ind].size() > right_ind){
			// left check first keeping the left_ind
			for (std::size_t i = 0; i < completion_matrix[left_ind].size(); i++){
				if (i != right_ind && !completion_matrix[left_ind][i]){
					return std::make_tuple(left_ind, (int)i);
				}
			}
			// now lets check keeping the right_ind
			for (std::size_t i = 0; i < completion_matrix.size(); i++){
				if (i != left_ind && !completion_matrix[i][right_ind]){
					return std::make_tuple((int)i, right_ind);
				}
			}
			return std::make_tuple(-1, -1);
		} else {
			std::cout<<"ERROR out of range in check_for_another_set_to_do_with_data_we_already_have"<<std::endl;
			return std::make_tuple(-1, -1);
		}
	}

	// This function returns the first not completed set, otherwise it returns [-1, -1]
	std::tuple<int, int> check_for_set_that_has_not_been_completed(){
		for (std::size_t i = 0; i < completion_matrix.size(); i++){
			for (std::size_t j = 0; j < completion_matrix[i].size(); j++){
			if (!completion_matrix[i][j]){
				return std::make_tuple((int)i, (int)j));
			}
		}
		return std::make_tuple(-1, -1);		
	}

	// this function makes sure that the columns being joined are of the same type so that we can join them properly
	void normalize(std::unique_ptr<ral::frame::BlazingTable> & left, std::unique_ptr<ral::frame::BlazingTable> & right){
		std::vector<std::unique_ptr<ral::frame::BlazingColumn>> left_columns = left->releaseBlazingColumns();
		std::vector<std::unique_ptr<ral::frame::BlazingColumn>> right_columns = right->releaseBlazingColumns();
		for (size_t i = 0; i < this->left_column_indices.size(); i++){
			if (left_columns[this->left_column_indices[i]]->view().type().id() != right_columns[this->right_column_indices[i]]->view().type().id()){
			std::vector<std::unique_ptr<ral::frame::BlazingColumn>> columns_to_normalize;
			columns_to_normalize.emplace_back(std::move(left_columns[this->left_column_indices[i]]));
			columns_to_normalize.emplace_back(std::move(right_columns[this->right_column_indices[i]]));
			std::vector<std::unique_ptr<ral::frame::BlazingColumn>> normalized_columns = ral::utilities::experimental::normalizeColumnTypes(std::move(columns_to_normalize));
			left_columns[this->left_column_indices[i]] = std::move(normalized_columns[0]);
			right_columns[this->right_column_indices[i]] = std::move(normalized_columns[1]);
			}
		}
		left = std::make_unique<ral::frame::BlazingTable>(std::move(left_columns), left->names());
		right = std::make_unique<ral::frame::BlazingTable>(std::move(right_columns), right->names());
	}

	std::unique_ptr<ral::frame::BlazingTable> join_set(const ral::frame::BlazingTableView & table_left, const ral::frame::BlazingTableView & table_right){
		  
		  std::unique_ptr<CudfTable> result_table;
		  std::vector<std::pair<cudf::size_type, cudf::size_type>> columns_in_common;
		  if(this->join_type == INNER_JOIN) {
			result_table = cudf::experimental::inner_join(
			table_left.view(),
			table_right.view(),
			this->left_column_indices,
			this->right_column_indices,
			columns_in_common);
		} else if(this->join_type == LEFT_JOIN) {
			result_table = cudf::experimental::left_join(
			table_left.view(),
			table_right.view(),
			this->left_column_indices,
			this->right_column_indices,
			columns_in_common);
		} else if(this->join_type == OUTER_JOIN) {
			result_table = cudf::experimental::full_join(
			table_left.view(),
			table_right.view(),
			this->left_column_indices,
			this->right_column_indices,
			columns_in_common);
		} else {
			RAL_FAIL("Unsupported join operator");
		}

		return std::make_unique<ral::frame::BlazingTable>(std::move(result_table), this->result_names);
	}


	virtual kstatus run() {
		this->left_sequence = BatchSequence(this->input_.get_cache("input_a"));
		this->right_sequence = BatchSequence(this->input_.get_cache("input_b"));

		// lets parse part of the expression here, because we need the joinType before we load
		std::string condition = get_named_expression(this->expression, "condition");
		this->join_type = get_named_expression(this->expression, "joinType");

		std::unique_ptr<ral::frame::BlazingTable> left_batch = load_left_set();
		std::unique_ptr<ral::frame::BlazingTable> right_batch = load_right_set();
		this->max_left_ind = 0; // we have loaded just once. This is the highest index for now
		this->max_right_ind = 0; // we have loaded just once. This is the highest index for now

		{ // parsing more of the expression here because we need to have the number of columns of the tables
			std::vector<int> column_indices;
			ral::processor::parseJoinConditionToColumnIndices(condition, column_indices);
			for(int i = 0; i < column_indices.size();i++){
				if(column_indices[i] >= left_batch->num_columns()){
					this->right_column_indices.push_back(column_indices[i] - left_batch->num_columns());
				}else{
					this->left_column_indices.push_back(column_indices[i]);
				}
			}
			std::vector<std::string> result_names = left_batch->names();
			std::vector<std::string> right_names = right_batch->names();
			this->result_names.insert(this->result_names.end(), right_names.begin(), right_names.end());
		}

		bool done = false;
		int left_ind = 0;
		int right_ind = 0;
		
		while (!done) {
			normalize(left_batch, right_batch);
			std::unique_ptr<ral::frame::BlazingTable> joined = join_set(left_batch->toBlazingTableView(), right_batch->toBlazingTableView());
			this->output_cache()->addToCache(std::move(joined));
			mark_set_completed(left_ind, right_ind);

			// We joined a set pair. Now lets see if there is another set pair we can do, but keeping one of the two sides we already have
			int new_left_ind, new_right_ind;
			std::tie(new_left_ind, new_right_ind) = check_for_another_set_to_do_with_data_we_already_have(left_ind, right_ind);
			if (new_left_ind >= 0 || new_right_ind >= 0) {
				if (new_left_ind >= 0) {
					leftArrayCache.put(left_ind, std::move(left_batch));
					left_ind = new_left_ind;
					left_batch = leftArrayCache.get(left_ind);
				} else {
					rightArrayCache.put(right_ind, std::move(right_batch));
					right_ind = new_right_ind;
					right_batch = rightArrayCache.get(right_ind);
				}
			} else {
				// lets try first to just grab the next one that is already available and waiting, but we keep one of the two sides we already have
				if (left_sequence.next_exists()){
					leftArrayCache.put(left_ind, std::move(left_batch));
					left_batch = load_left_set();
					left_ind = this->max_left_ind;
				} else if (right_sequence.next_exists()){
					rightArrayCache.put(right_ind, std::move(right_batch));
					right_batch = load_right_set();
					right_ind = this->max_right_ind;
				} else {
					// lets see if there are any in are matrix that have not been completed
					std::tie(new_left_ind, new_right_ind) = check_for_set_that_has_not_been_completed();
					if (new_left_ind >= 0 && new_right_ind >= 0) {
						leftArrayCache.put(left_ind, std::move(left_batch));
						left_ind = new_left_ind;
						left_batch = leftArrayCache.get(left_ind);
						rightArrayCache.put(right_ind, std::move(right_batch));
						right_ind = new_right_ind;
						right_batch = rightArrayCache.get(right_ind);
					} else {
						// nothing else for us to do buy wait and see if there are any left to do
						if (left_sequence.has_next()){
							leftArrayCache.put(left_ind, std::move(left_batch));
							left_batch = load_left_set();
							left_ind = this->max_left_ind;
						} else if (right_sequence.has_next()){
							rightArrayCache.put(right_ind, std::move(right_batch));
							right_batch = load_right_set();
							right_ind = this->max_right_ind;
						} else {
							done = true;
						}
					}
				}
			}
		}

		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
	std::vector<std::vector<bool>> completion_matrix;
	BatchSequence left_sequence, right_sequence;
	ArrayCache leftArrayCache, rightArrayCache;
	std::size_t SET_SIZE_THRESHOLD;

	// parsed expression related parameters
	std::string join_type;
	std::vector<cudf::size_type> left_column_indices, right_column_indices;
  	std::vector<std::string> result_names;
};


class JoinPartitionKernel : public PhysicalPlan {
public:
	JoinPartitionKernel(const std::string & queryString, std::shared_ptr<Context> context)
		: expression{queryString}, context{context} {

		this->input_.add_port("input_a", "input_b");
		this->output_.add_port("output_a", "output_b");
	}

	static partition_table(std::shared_ptr<Context> context, 
				std::vector<cudf::size_type> column_indices, 
				std::unique_ptr<ral::frame::BlazingTable> batch, 
				BatchSequence & sequence, 
				std::shared_ptr<CacheMachine> & output){

		bool done = false;
		while (!done) {
			// num_partitions = context->getTotalNodes() will do for now, but may want a function to determine this in the future. 
            // If we do partition into something other than the number of nodes, then we have to use part_ids and change up more of the logic
            int num_partitions = context->getTotalNodes(); 
            std::unique_ptr<CudfTable> hashed_data;
            std::vector<cudf::size_type> hased_data_offsets;
            std::tie(hashed_data, hased_data_offsets) = cudf::hash_partition(batch->view(), column_indices, num_partitions);

            // the offsets returned by hash_partition will always start at 0, which is a value we want to ignore for cudf::split
            std::vector<cudf::size_type> split_indexes(hased_data_offsets.begin() + 1, hased_data_offsets.end());
            std::vector<CudfTableView> partitioned = cudf::experimental::split(hashed_data->view(), split_indexes);

            std::vector<ral::distribution::experimental::NodeColumnView > partitions_to_send;
            for(int nodeIndex = 0; nodeIndex < context->getTotalNodes(); nodeIndex++ ){
                ral::frame::BlazingTableView partition_table_view = ral::frame::BlazingTableView(partitioned[nodeIndex], batch->names());
                if (context->getNode(nodeIndex) == ral::communication::experimental::CommunicationData::getInstance().getSelfNode()){
                    // hash_partition followed by split does not create a partition that we can own, so we need to clone it.
                    // if we dont clone it, hashed_data will go out of scope before we get to use the partition
                    // also we need a BlazingTable to put into the cache, we cant cache views.
                    std::unique_ptr<ral::frame::BlazingTable> partition_table_clone = partition_table_view.clone();
                    output->addToCache(std::move(partition_table_clone));
                } else {
                    partitions_to_send.emplace_back(
                        std::make_pair(context->getNode(nodeIndex), partition_table_view));
                }
            }
			// WSM need to figure out how to send only to one side (left or right)
            ral::distribution::experimental::distributeTablePartitions(context.get(), partitions_to_send);			

			if (sequence.wait_for_next()){
				batch = sequence.next();
			} else {
				done = true;
			}
		}
		// WSM notify here
	}

	virtual kstatus run() {
		
		BatchSequence left_sequence(this->input_.get_cache("input_a"));
		BatchSequence right_sequence(this->input_.get_cache("input_b"));

		// lets parse part of the expression here, because we need the joinType before we load
		std::string condition = get_named_expression(this->expression, "condition");
		this->join_type = get_named_expression(this->expression, "joinType");

		std::unique_ptr<ral::frame::BlazingTable> left_batch = left_sequence.next();
		std::unique_ptr<ral::frame::BlazingTable> right_batch = right_sequence.next();

		if (left_batch == nullptr || left_batch->num_columns() == 0){
			std::cout<<"ERROR JoinPartitionKernel has empty left side and cannot determine join column indices"<<std::endl;
		}
		
		{ // parsing more of the expression here because we need to have the number of columns of the tables
			std::vector<int> column_indices;
			ral::processor::parseJoinConditionToColumnIndices(condition, column_indices);
			for(int i = 0; i < column_indices.size();i++){
				if(column_indices[i] >= left_batch->num_columns()){
					this->right_column_indices.push_back(column_indices[i] - left_batch->num_columns());
				}else{
					this->left_column_indices.push_back(column_indices[i]);
				}
			}
		}

		std::thread distribute_left_thread(&JoinPartitionKernel::partition_table, context, 
			this->left_column_indices, std::move(left_batch), std::ref(left_sequence), 
			std::ref(this->output_.get_cache("output_a")));
		
		std::thread distribute_right_thread(&JoinPartitionKernel::partition_table, context, 
			this->right_column_indices, std::move(right_batch), std::ref(right_sequence), 
			std::ref(this->output_.get_cache("output_b")));

		distribute_left_thread.join();
		distribute_right_thread.join();

		
		// WSM how do I do this for the two sides?
		ExternalBatchColumnDataSequence external_input(context);
		std::unique_ptr<ral::frame::BlazingHostTable> host_table;
		while (host_table = external_input.next()) {	
            this->output_cache()->addHostFrameToCache(std::move(host_table));
		}

		return kstatus::proceed;
	}

private:
	std::shared_ptr<Context> context;
	std::string expression;
	
	// parsed expression related parameters
	std::string join_type;
	std::vector<cudf::size_type> left_column_indices, right_column_indices;
};



} // namespace batch
} // namespace ral




// single node
/*


partwiseJoin

loads should load a "min" amount. If we have too many nodes, the parts are too small
too many small parts becomes too many small joins

load A0, load B0
join A0, B0
load B1
join A0, B1
load BN
join A0, BN
load A1


left outer join
left side we do iteratively, right side we do the whole thing

full outer join
right and left we do the whole thing



