
#include "protocols.hpp"

#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>

#include <map>
#include <vector>

namespace comm {

// TODO: remove this hack when we can modify the ucx_request
// object so we cna send in our c++ callback via the request
std::map<int, ucx_buffer_transport *> uid_to_buffer_transport;

/**
 * A struct that lets us access the request that the end points ucx-py generates.
 */
struct ucx_request {
	//!!!!!!!!!!! do not modify this struct this has to match what is found in
	// https://github.com/rapidsai/ucx-py/blob/branch-0.15/ucp/_libs/ucx_api.pyx
	// Make sure to check on the latest branch !!!!!!!!!!!!!!!!!!!!!!!!!!!
	int completed; /**< Completion flag that we do not use. */
	int uid;	   /**< We store a map of request uid ==> buffer_transport to manage completion of send */
};

void send_begin_callback_c(void * request, ucs_status_t status) {
	auto blazing_request = reinterpret_cast<ucx_request *>(request);
	auto transport = uid_to_buffer_transport[blazing_request->uid];
	transport->increment_begin_transmission();
	ucp_request_release(request);
}

void send_callback_c(void * request, ucs_status_t status) {
	auto blazing_request = reinterpret_cast<ucx_request *>(request);
	auto transport = uid_to_buffer_transport[blazing_request->uid];
	transport->increment_frame_transmission();
	ucp_request_release(request);
}


ucx_buffer_transport::ucx_buffer_transport(node origin_node,
    std::vector<node> destinations,
	ral::cache::MetadataDictionary metadata,
	std::vector<size_t> buffer_sizes,
	std::vector<blazingdb::transport::ColumnTransport> column_transports)
	: buffer_transport(metadata, buffer_sizes, column_transports), transmitted_begin_frames(0), transmitted_frames(0),
	  origin_node(origin_node), destinations{destinations} {
	tag = generate_message_tag();
}

/**
 * A struct for managing the 64 bit tag that ucx uses
 * This allow us to make a value that is stored in 8 bytes
 */
struct blazing_ucp_tag {
	int message_id;			   /**< The message id which is generated by a global atomic*/
	uint16_t worker_origin_id; /**< The id to make sure each tag is unique */
	uint16_t frame_id;		   /**< The id of the frame being sent. 0 for being_transmission*/
};

std::atomic<int> message_id(0);

ucp_tag_t ucx_buffer_transport::generate_message_tag() {
	blazing_ucp_tag blazing_tag = {message_id.fetch_add(1), origin_node.index(), 0u};
	return *reinterpret_cast<ucp_tag_t *>(&blazing_tag);
}

void ucx_buffer_transport::send_begin_transmission() {
	std::vector<char> buffer_to_send = make_begin_transmission();

	std::vector<ucs_status_ptr_t> requests;
	for(auto const & node : destinations) {
		requests.push_back(ucp_tag_send_nb(
			node.get_ucp_endpoint(), buffer_to_send.data(), buffer_to_send.size(), ucp_dt_make_contig(1), tag, send_begin_callback_c));
	}

	for(auto & request : requests) {
		if(UCS_PTR_IS_ERR(request)) {
			// TODO: decide how to do cleanup i think we just throw an initialization exception
		} else if(UCS_PTR_STATUS(request) == UCS_OK) {
			ucp_request_release(request);
			increment_begin_transmission();
		} else {
			// Message was not completed we set the uid for the callback
			auto blazing_request = reinterpret_cast<ucx_request *>(&request);
			blazing_request->uid = reinterpret_cast<blazing_ucp_tag *>(&tag)->message_id;
		}
	}
	// TODO: call ucp_worker_progress here
	wait_for_begin_transmission();
}

void ucx_buffer_transport::increment_frame_transmission() {
	transmitted_frames++;
	completion_condition_variable.notify_all();
}

void ucx_buffer_transport::increment_begin_transmission() {
	transmitted_begin_frames++;
	completion_condition_variable.notify_all();
}

void ucx_buffer_transport::wait_for_begin_transmission() {
	std::unique_lock<std::mutex> lock(mutex);
	completion_condition_variable.wait(lock, [this] {
		if(transmitted_begin_frames >= destinations.size()) {
			return true;
		} else {
			return false;
		}
	});
}

void ucx_buffer_transport::wait_until_complete() {
	std::unique_lock<std::mutex> lock(mutex);
	completion_condition_variable.wait(lock, [this] {
		if(transmitted_frames >= (buffer_sizes.size() * destinations.size())) {
			return true;
		} else {
			return false;
		}
	});
}

void ucx_buffer_transport::send_impl(const char * buffer, size_t buffer_size) {
	std::vector<ucs_status_ptr_t> requests;
	blazing_ucp_tag blazing_tag = *reinterpret_cast<blazing_ucp_tag *>(&tag);
	blazing_tag.frame_id = buffer_sent + 1;	 // 0th position is for the begin_message
	for(auto const & node : destinations) {
		requests.push_back(ucp_tag_send_nb(node.get_ucp_endpoint(),
			buffer,
			buffer_size,
			ucp_dt_make_contig(1),
			*reinterpret_cast<ucp_tag_t *>(&blazing_tag),
			send_callback_c));
	}

	for(auto & request : requests) {
		if(UCS_PTR_IS_ERR(request)) {
			// TODO: decide how to do cleanup i think we just throw an initialization exception
		} else if(UCS_PTR_STATUS(request) == UCS_OK) {
			ucp_request_release(request);
			transmitted_begin_frames++;
		} else {
			// Message was not completed we set the uid for the callback
			auto blazing_request = reinterpret_cast<ucx_request *>(&request);
			blazing_request->uid = reinterpret_cast<blazing_ucp_tag *>(&tag)->message_id;
		}
	}
	// TODO: call ucp_worker_progress here
}

ucx_message_listener::ucx_message_listener(ucp_worker_h worker) :
    worker(worker) {

}
std::pair<ral::cache::MetadataDictionary, std::vector<ColumnTransport> > get_metadata_and_transports_from_bytes(std::vector<char> data){
    
}

std::map<int, std::shared_ptr<message_receiver> > 
void ucx_message_listener::poll_begin_message_tag(ucp_tag_t tag, ucp_tag_t mask){
    for(;;;){
        ucp_tag_recv_info_t info_tag;
        auto message_tag = ucp_tag_probe_nb(
            ucp_worker, tag, mask, 1, &info_tag);
        if(message != NULL){
            //we have a msg to process
            std::vector<char> message_metadata(info_tag.length);
            auto request = ucp_tag_msg_recv_nb(ucp_worker, message_metadata.data(), info_tag.length,
                                  ucp_dt_make_contig(1), message_tag,
                                  recv_handler);
            
            message_receiver(const std::vector<ColumnTransport> & column_transports,
                  const ral::cache::MetadataDictionary & metadata,
                  bool include_metadata,
                  std::shared_ptr<ral::cache::CacheMachine> output_cache)
            continue;
        }else(ucp_worker_progress(ucp_worker)) {
            continue; //messages came in we can check again
        }

        if(message == NULL){
            //waits until a message event occurs
            auto status = ucp_worker_wait(ucp_worker);
            if (status != UCS_OK){
                throw ::std::exception()
            }
        }        

    }

}

}  // namespace comm
