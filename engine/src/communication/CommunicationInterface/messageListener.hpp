#pragma once

#include <memory>
#include <map>

#include "utilities/ctpl_stl.h"
#include "blazingdb/concurrency/BlazingThread.h"
#include <ucp/api/ucp.h>
#include <ucp/api/ucp_def.h>
#include "messageReceiver.hpp"

namespace comm {

class message_listener{
public:
    message_listener();
    virtual ~message_listener(){

    }
    virtual void start_polling() = 0;
    ctpl::thread_pool<BlazingThread> & get_pool();
    std::map<std::string, comm::node> get_node_map();
    bool isInitialized() { return is_initialized;}

protected:
    ctpl::thread_pool<BlazingThread> pool;
    std::map<std::string, comm::node> _nodes_info_map;
    bool is_initialized{false};
    bool pooling_started{false};
};

class tcp_message_listener : public message_listener {

public:
    static tcp_message_listener& get_instance(){
        // Myers' singleton. Thread safe and unique. Note: C++11 required.
        static tcp_message_listener instance;
        return instance;
    }
    void initialize_message_listener(const std::map<std::string, comm::node>& nodes, int port, int num_threads);
    void start_polling() override;
    virtual ~tcp_message_listener(){

    }
private:
    tcp_message_listener();
    int _port;    
};



class ucx_message_listener : public message_listener {
public:

    static ucx_message_listener& get_instance(){
        // Myers' singleton. Thread safe and unique. Note: C++11 required.
        static ucx_message_listener instance;
        return instance;
    }

    void initialize_message_listener(ucp_context_h context, ucp_worker_h worker, const std::map<std::string, comm::node>& nodes, int num_threads);
    
    
    void poll_begin_message_tag(bool running_from_unit_test);
    void add_receiver(ucp_tag_t tag,std::shared_ptr<message_receiver> receiver);
    void remove_receiver(ucp_tag_t tag);
    void increment_frame_receiver(ucp_tag_t tag);
    ucp_worker_h get_worker();
    void start_polling() override;
private:
    ucx_message_listener();
	virtual ~ucx_message_listener(){

    }
    void poll_message_tag(ucp_tag_t tag, ucp_tag_t mask);
    size_t _request_size;
    ucp_worker_h ucp_worker;
    std::map<ucp_tag_t,std::shared_ptr<message_receiver> > tag_to_receiver;
	
};

} // namespace comm
