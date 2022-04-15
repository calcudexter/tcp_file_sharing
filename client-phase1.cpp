#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <signal.h>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <thread>
#include <vector>
#include <algorithm>
#include <map>
#include <set>

using namespace std;
namespace fs = std::filesystem;

#define ll long long
#define BACKLOG 10

void sigchld_handler(int s) {
    // waitpid() might overwrite errno, so we save and restore it:
    int saved_errno = errno;

    while(waitpid(-1, NULL, WNOHANG) > 0);

    errno = saved_errno;
}


// get sockaddr, IPv4 or IPv6:
void *get_in_addr(struct sockaddr *sa) {
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

int main(int argc, char **argv) {
    
    string config_file = argv[1];
    string my_dir = argv[2];

    fstream fin;
    string line;

    int id, port, uid;
    fin.open(config_file);
    fin >> id >> port >> uid;

    vector<string> my_files;    

    for(const auto &entry : fs::directory_iterator(my_dir)) {
        string fname = entry.path().filename();

        my_files.push_back(fname);
    }

    sort(my_files.begin(), my_files.end());

    for(auto f : my_files) {
        cout << f << endl;
    }

    int num_neighbors;
    fin >> num_neighbors;

    vector<pair<int, int>> neighbors;
    // id, port
    
    map<int, tuple<int, int, int>> neighbor_info;
    // Info about the neighbor given its ID
    // Structure : {socket_file_descriptor, port, UID}

    set<int> unconnected;

    for(ll i = 0; i < num_neighbors; i++) {
        ll n_id, n_port;
        fin >> n_id >> n_port;

        neighbors.push_back({n_id, n_port});
        unconnected.insert(n_id);
    }

    int num_files_down;
    fin >> num_files_down;

    string search_files[num_files_down];

    for(ll i = 0; i < num_files_down; i++) {
        fin >> search_files[i];
    }

    // ----------------------
    // Socket programming starts here

    fd_set master;
    fd_set read_fds;
    int fdmax;

    int listener;
    int newfd;
    struct sockaddr_storage remoteaddr;
    socklen_t addrlen;

    char buf[256];
    int nbytes;

    char remoteIP[INET6_ADDRSTRLEN];

    int yes = 1;
    int i, j, rv;

    struct addrinfo hints, *ai, *servinfo, *p;

    FD_ZERO(&master);
    FD_ZERO(&read_fds);

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if((rv = getaddrinfo(NULL, to_string(port).c_str(), &hints, &ai)) != 0) {
        // fprintf(stderr, "selectserver: %s\n", gai_strerror(rv));
        exit(1);
    }

    for(p = ai; p != NULL; p = p->ai_next) {
        listener = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if(listener < 0) {
            continue;
        }

        setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));

        if(bind(listener, p->ai_addr, p->ai_addrlen) < 0) {
            close(listener);
            continue;
        }
        
        break;
    }

    if(p == NULL) {
        // fprintf(stderr, "selectserver: failed to bind\n");
        exit(2);
    }

    freeaddrinfo(ai);

    if(listen(listener, 10) == -1) {
        // perror("listen");
        exit(3);
    }

    FD_SET(listener, &master);

    fdmax = listener;

    for(ll i = 0; i < num_neighbors; i++) {
        // cout << "Sending request to " << neighbors[i].first << " on the port " << neighbors[i].second << endl;
        
        if((rv = getaddrinfo(NULL, to_string(neighbors[i].second).c_str(), &hints, &servinfo)) != 0) {
            // fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
            return 1;
        }

        for(p = servinfo; p != NULL; p = p->ai_next) {
            int client_sockfd;
            if ((client_sockfd = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1) {
                // perror("client: socket");
                continue;
            }

            if(connect(client_sockfd, p->ai_addr, p->ai_addrlen) == -1) {
                close(client_sockfd);
                // perror("client: connect");
                continue;
            }

            string message = to_string(id) + "," + to_string(port) + "," + to_string(uid);
            if(send(client_sockfd, message.c_str(), 16, 0) == -1) {
                // perror("send");
            }

            neighbor_info[neighbors[i].first] = {client_sockfd, neighbors[i].second, -1};
            fdmax = max(fdmax, client_sockfd);
            FD_SET(client_sockfd, &master);

            break;
        }
        if(p == NULL) {
            // fprintf(stderr, "client: failed to connect to %d\n", neighbors[i].first);
        }

        freeaddrinfo(servinfo);
    }

    for(;;) {
        read_fds = master;

        if(select(fdmax+1, &read_fds, NULL, NULL, NULL) == -1) {
            // perror("select");
            exit(4);
        }

        for(i = 0; i <= fdmax; i++) {
            if(FD_ISSET(i, &read_fds)) {
                if(i == listener) {
                    addrlen = sizeof remoteaddr;
                    newfd = accept(listener, (struct sockaddr *)&remoteaddr, &addrlen);

                    if(newfd == -1) {
                        // perror("accept");
                    }
                    else {
                        FD_SET(newfd, &master);

                        if(newfd > fdmax) {
                            fdmax = newfd;
                        }

                        string message = to_string(id) + "," + to_string(port) + "," + to_string(uid);
                        if(send(newfd, message.c_str(), 16, 0) == -1) {
                            // perror("send");
                        }

                        // printf("selectserver: new connection from %s on socket %d\n", inet_ntop(remoteaddr.ss_family, get_in_addr((struct sockaddr *)&remoteaddr), remoteIP, INET6_ADDRSTRLEN), newfd);
                    }
                }
                else {
                    if((nbytes = recv(i, buf, sizeof buf, 0)) <= 0) {
                        if(nbytes == 0) {
                            // printf("selectserver: socket %d hung up\n", i);
                        }
                        else {
                            // perror("recv");
                        }
                        close(i);
                        FD_CLR(i, &master);
                    }
                    else {
                        // Successful reception of data
                        // Parse the buffer and go ahead

                        // printf("I received '%s' on socket %d\n", buf, i);

                        string message(buf, buf+nbytes);

                        int n_id, n_port, n_uid;
                        int commas[2], itr = 0;
                        int len = message.length();

                        for(int i = 0; i < len; i++) {
                            if(message[i] == ',') commas[itr++] = i;
                            if(itr == 2) break;
                        }

                        n_id = stoi(message.substr(0, commas[0]));
                        n_port = stoi(message.substr(commas[0]+1, commas[1]-(commas[0]+1)));
                        n_uid = stoi(message.substr(commas[1]+1, len-(commas[1]+1)));

                        get<2>(neighbor_info[n_id]) = n_uid;

                        unconnected.erase(n_id);
                        if(unconnected.empty())
                            break;
                    }
                }
            }
        }
    
    
        if(unconnected.empty())
            break;
    }

    for(auto neighbor : neighbors) {
        int n_id = neighbor.first;
        int n_port = neighbor.second;
        int n_uid = get<2>(neighbor_info[n_id]);

        cout << "Connected to " << n_id << " with unique-ID " << n_uid << " on port " << n_port << endl;
    }

    return 0;
}