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
#include <sys/stat.h>
#include <openssl/md5.h>
#include <queue>

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

vector<string> splitstring(string s) {
    int lc = 0;
    vector<string> out;

    s = "," + s + ",";
    int len = s.length();

    for(int i = 1; i < len; i++) {
        if(s[i] == ',') {
            out.push_back(s.substr(lc+1, (i-lc-1)));
            lc = i;
        }
    }

    return out;
}

vector<string> splitmsg(string s) {
    int lc = 0;
    vector<string> out;

    s = '$' + s;
    int len = s.length();

    for(int i = 1; i < len; i++) {
        if(s[i] == '$') {
            out.push_back(s.substr(lc+1, (i-lc-1)));
            lc = i;
        }
    }

    return out;
}

int main(int argc, char **argv) {
    std::cout.flush();
    
    string config_file = argv[1];
    string my_dir = argv[2];

    fstream fin;
    string line;

    int id, port, uid;
    fin.open(config_file);
    fin >> id >> port >> uid;

    sleep(2*id);

    // cout << "I am " << uid << endl;

    vector<string> my_files;
    set<string> my_files_set;

    for(const auto &entry : fs::directory_iterator(my_dir)) {
        string fname = entry.path().filename();

        my_files.push_back(fname);
        my_files_set.insert(fname);
    }

    sort(my_files.begin(), my_files.end());

    for(auto f : my_files) {
        cout << f << endl;
    }

    int num_neighbors;
    fin >> num_neighbors;

    vector<pair<int, int>> neighbors;
    // {id, port}
    
    map<int, tuple<int, int, int>> neighbor_info;
    // Info about the neighbor given its ID
    // Structure : {socket_file_descriptor, port, UID}
    
    // map<int, vector<string>> chats;
    // The chat history of the neighbor with given ID

    map<int, int> sockfd2ID;
    map<int, int> uid2sockfd;

    // set<int> unconnected;
    // bool all_connect = false;
    set<int> unresponded;
    set<int> unrequested;
    set<int> d2neighbors;
    map<int, int> d2neighbor_status;
    // UID to Status
    set<int> d2connected;

    map<int, int> d2_uid2outsockfd;
    map<int, int> d2_outsockfd2id;

    map<int, queue<string>> sockfd2filereq;
    // map<int, tuple<int, int, int>> d2neighbor_info;

    bool all_responded = false, all_requested = false, all_2responded = false;

    int expected_2REQs = 0, expected_2RESPs = num_neighbors*(num_neighbors-1), expected_DRESPs = 0;
    int received_2REQs = 0, received_2RESPs = 0, received_DRESPs = 0;

    // bool file_in = false;
    map<int, bool> file_in;
    // Map from socketfd to boolean
    // int rem_bytes;

    map<int, int> rem_bytes;
    // Map from sockfd to int

    // string infile_name;
    map<int, string> infile_name;
    // Map from sockfd to string

    map<string, string> file_hash;
    map<string, string> file_depth;
    map<string, string> file_owneruid;
    set<string> wanted_files;

    int all_got = 0;
    int d2sent = 0;
    int files_needed = 0, files_received = 0;

    vector<string> to_be_sentReqs;

    for(ll i = 0; i < num_neighbors; i++) {
        ll n_id, n_port;
        fin >> n_id >> n_port;

        neighbors.push_back({n_id, n_port});
        unresponded.insert(n_id);
        unrequested.insert(n_id);
    }

    int num_files_down;
    fin >> num_files_down;

    string search_files[num_files_down];

    map<string, set<int>> file_owners;
    map<string, set<tuple<int, int, int>>> d2file_owners;
    // This should contain the {uid, id and port} of that file owner

    for(ll i = 0; i < num_files_down; i++) {
        fin >> search_files[i];
    }

    sort(search_files, search_files+num_files_down);

    // ----------------------
    // Socket programming starts here

    fd_set master;
    fd_set read_fds;
    fd_set write_fds;
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
    FD_ZERO(&write_fds);

    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if((rv = getaddrinfo(NULL, to_string(port).c_str(), &hints, &ai)) != 0) {
        // fprintf(stderr, "selectserver: %s$", gai_strerror(rv));
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
        // fprintf(stderr, "selectserver: failed to bind$");
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
            // fprintf(stderr, "getaddrinfo: %s$", gai_strerror(rv));
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

            string message = "INFO," + to_string(id) + "," + to_string(port) + "," + to_string(uid) + "," + to_string(num_neighbors) + '$';
            if(send(client_sockfd, message.c_str(), message.length()+1, 0) == -1) {
                // perror("send");
            }

            message = "REQ," + to_string(uid) + "," + to_string(id) + "," + to_string(num_files_down);
            int len = message.length();

            for(auto file : search_files) {
                message += "," + file;
            }
            
            message += '$';
            if(send(client_sockfd, message.c_str(), message.length()+1, 0) == -1) {
                // perror("send");
            }

            neighbor_info[neighbors[i].first] = {client_sockfd, neighbors[i].second, -1};
            sockfd2ID[client_sockfd] = neighbors[i].first;
            fdmax = max(fdmax, client_sockfd);
            FD_SET(client_sockfd, &master);

            break;
        }
        if(p == NULL) {
            // fprintf(stderr, "client: failed to connect to %d$", neighbors[i].first);
        }

        freeaddrinfo(servinfo);
    }

    bool printed = false;
    bool hashed = false;

    if(num_neighbors > 0) {
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

                            // string message = "INFO," + to_string(id) + "," + to_string(port) + "," + to_string(uid) + '$';
                            string message = "INFO," + to_string(id) + "," + to_string(port) + "," + to_string(uid) + "," + to_string(num_neighbors) + '$';
                            if(send(newfd, message.c_str(), message.length()+1, 0) == -1) {
                                // perror("send");
                            }

                            message = "REQ," + to_string(uid) + "," + to_string(id) + "," + to_string(num_files_down);

                            int len = message.length();

                            for(auto file : search_files) {
                                message += "," + file;
                            }

                            message += '$';

                            if(send(newfd, message.c_str(), message.length()+1, 0) == -1) {
                                perror("send");
                            }

                            // printf("selectserver: new connection from %s on socket %d$", inet_ntop(remoteaddr.ss_family, get_in_addr((struct sockaddr *)&remoteaddr), remoteIP, INET6_ADDRSTRLEN), newfd);
                        }
                    }
                    else {
                        if((nbytes = recv(i, buf, sizeof buf, 0)) <= 0) {
                            if(nbytes == 0) {
                                // printf("selectserver: socket %d hung up$", i);
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

                            // printf("I received '%s' on socket %d$", buf, i);

                            string message(buf, buf+nbytes);
                            // cout << "Got this message " << message << endl << endl;
                            // if(sockfd2ID[i] == 4) cout << message << endl;

                            if(!file_in[i]) {
                                int fsind = 0;
                                vector<string> msgs = splitmsg(message);

                                // Loop to clean the messages
                                for(int i = 1; i < msgs.size(); i++) {
                                    if(msgs[i].length())
                                        msgs[i] = msgs[i].substr(1, msgs[i].length()-1);
                                }

                                // chats[sockfd2ID[i]].insert(chats[sockfd2ID[i]].end(), msgs.begin(), msgs.end());
                                
                                for(auto msg : msgs) {
                                    vector<string> words = splitstring(msg);

                                    // if(sockfd2ID[i] == 4 && id == 5) cout << "The first word was " << words[0] << endl;

                                    if(words[0] == "INFO") {
                                        int n_id, n_port, n_uid, n_num_neigh;

                                        n_id = stoi(words[1]);
                                        n_port = stoi(words[2]);
                                        n_uid = stoi(words[3]);
                                        n_num_neigh = stoi(words[4]);

                                        bool d2 = true;
                                        for(auto n : neighbors) {
                                            if(n.first == n_id) d2 = false;
                                        }

                                        if(d2) continue;

                                        expected_2REQs += (n_num_neigh-1);
                                        expected_DRESPs += (n_num_neigh-1);
                                        
                                        sockfd2ID[i] = n_id;
                                        uid2sockfd[n_uid] = i;

                                        get<0>(neighbor_info[n_id]) = i;
                                        get<2>(neighbor_info[n_id]) = n_uid;
                                    }
                                    else {
                                        if(words[0] == "REQ") {
                                            int n_uid = stoi(words[1]), n_id = stoi(words[2]), num_files = stoi(words[3]);
                                            bool d2 = true;
                                            for(auto n : neighbors) {
                                                if(n.first == n_id) d2 = false;
                                            }

                                            if(d2) continue;

                                            string message = "RESP," + to_string(uid) + "," + to_string(id) +"," + to_string(num_files);

                                            for(int i = 0; i < num_files; i++) {
                                                string fn = words[4+i];

                                                if(my_files_set.count(fn)) message += ",YES";
                                                else message += ",NO";
                                            }

                                            message += '$';

                                            if(send(i, message.c_str(), message.length()+1, 0) == -1) {
                                                perror("send");
                                            }

                                            // Send this message to all of its neighbors too except the requestor
                                            string forward = "2REQ," + to_string(uid) + "," + to_string(id) + "," + msg.substr(4, msg.length()-4) + '$';
                                            to_be_sentReqs.push_back(forward);

                                            unrequested.erase(n_id);
                                            if(unrequested.empty()) {
                                                all_requested = true;

                                                for(int ind = 0; ind < num_neighbors; ind++) {
                                                    int neigh_id = neighbors[ind].first;

                                                    for(int jnd = 0; jnd < to_be_sentReqs.size(); jnd++) {
                                                        vector<string> parts = splitstring(to_be_sentReqs[jnd]);

                                                        int n_id = stoi(parts[4]);
                                                        if(neigh_id == n_id) continue;

                                                        int n_sockfd = get<0>(neighbor_info[neigh_id]);

                                                        if(send(n_sockfd, to_be_sentReqs[jnd].c_str(), to_be_sentReqs[jnd].length()+1, 0) == -1) {
                                                            perror("send");
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        else if(words[0] == "RESP") {
                                            int n_uid = stoi(words[1]), n_id = stoi(words[2]), num_files = stoi(words[3]);

                                            int curr = 4;
                                            
                                            for(int i = 0; i < num_files; i++) {
                                                string resp = words[curr++];

                                                if(resp == "YES") {
                                                    file_owners[search_files[i]].insert(n_uid);
                                                }
                                            }
                                        
                                            unresponded.erase(n_id);
                                            if(unresponded.empty()) all_responded = true;
                                        }
                                        else if(words[0] == "2REQ") {
                                            received_2REQs++;
                                            int hop_uid = stoi(words[1]), hop_id = stoi(words[2]), src_uid = stoi(words[3]), src_id = stoi(words[4]), num_files = stoi(words[5]);

                                            string message = "2RESP," + to_string(uid) + "," + to_string(id) + "," + to_string(port) + "," + to_string(src_uid) + "," + to_string(src_id) + "," + to_string(num_files);

                                            for(int i = 0; i < num_files; i++) {
                                                string fn = words[6+i];

                                                if(my_files_set.count(fn)) message += ",YES";
                                                else message += ",NO";
                                            }

                                            message += '$';

                                            int n_sockfd = get<0>(neighbor_info[hop_id]);

                                            if(send(n_sockfd, message.c_str(), message.length()+1, 0) == -1) {
                                                perror("send");
                                            }

                                        }
                                        else if(words[0] == "2RESP") {
                                            received_2RESPs++;
                                            // Send this to the src again directly
                                            int src_uid = stoi(words[1]), src_id = stoi(words[2]), src_port = stoi(words[3]), dest_uid = stoi(words[4]), dest_id = stoi(words[5]), num_files = stoi(words[6]);

                                            string message = "DRESP," + words[1] + "," + words[2] + "," + words[3];

                                            for(int ind = 6; ind < words.size(); ind++) {
                                                message += "," + words[ind];
                                            }

                                            message += "," + to_string(num_neighbors-1);

                                            for(int ind = 0; ind < num_neighbors; ind++) {
                                                if(neighbors[ind].first == dest_id) continue;
                                                
                                                message += "," + to_string(get<2>(neighbor_info[neighbors[ind].first]));
                                            }

                                            message += '$';

                                            int n_sockfd = get<0>(neighbor_info[dest_id]);

                                            if(send(n_sockfd, message.c_str(), message.length()+1, 0) == -1) {
                                                perror("send");
                                            }

                                            d2sent++;
                                        }
                                        else if(words[0] == "DRESP") {
                                            received_DRESPs++;
                                            // Update the ownership
                                            int src_uid = stoi(words[1]), src_id = stoi(words[2]), src_port = stoi(words[3]), num_files = stoi(words[4]);

                                            int curr = 5;

                                            for(int ind = 0; ind < num_files; ind++) {
                                                string resp = words[curr++];

                                                if(resp == "YES") {
                                                    d2file_owners[search_files[ind]].insert({src_uid, src_id, src_port});
                                                }
                                            }

                                            int d2n = stoi(words[curr++]);

                                            for(int ind = 0; ind < d2n; ind++) {
                                                int d2n_uid = stoi(words[curr++]);

                                                d2neighbors.insert(d2n_uid);
                                                if(!d2neighbor_status[d2n_uid]) d2neighbor_status[d2n_uid] = 1;
                                            }

                                            d2neighbor_status[src_uid] = 2;
                                            
                                            all_2responded = true;
                                            for(auto d2nb : d2neighbors) {
                                                // cout << "Checking for the neighbor " << d2nb << endl;
                                                if(d2neighbor_status[d2nb] == 1) all_2responded = false;
                                            }
                                        }
                                        else if(words[0] == "FILEREQ") {
                                            // cout << msg << endl;
                                            int n_uid = stoi(words[1]), n_id = stoi(words[2]);
                                            string fn = words[3];

                                            uintmax_t fsz = fs::file_size(my_dir + fn);
                                            
                                            string message = "FILENEXT," + fn + "," + to_string(fsz) + '$';

                                            if(send(i, message.c_str(), message.length()+1, 0) == -1) {
                                                perror("send");
                                            }

                                            FILE *fd = fopen((my_dir + fn).c_str(), "rb");

                                            char file_data[128];

                                            size_t num_bytes = 0;
                                            while ( (num_bytes = fread(file_data, sizeof(char), 127, fd)) > 0)
                                            {
                                                int offset = 0, sent;

                                                while ((sent = send(i, file_data + offset, num_bytes, 0)) > 0
                                                    || (sent == -1 && errno == EINTR) ) {
                                                        if (sent > 0) {
                                                            offset += sent;
                                                            num_bytes -= sent;
                                                        }
                                                }
                                            }
                                        }
                                        else if(words[0] == "FILENEXT") {
                                            // if(id == 4) cout << msg << endl;
                                            file_in[i] = true;
                                            rem_bytes[i] = stoi(words[2]);
                                            infile_name[i] = words[1];
                                            all_got++;
                                            break;
                                        }
                                        // else {
                                            // cout << "Received this : " << msg << endl;
                                        // }
                                    }
                                }

                                if(file_in[i]) {
                                    for(int i = 0; i < message.length(); i++) {
                                        if(message.substr(i, 8) == "FILENEXT") {
                                            fsind = i;
                                            break;
                                        }
                                    }
                                    for(int i = fsind; i < message.length(); i++) {
                                        if(buf[i] == '\0') {
                                            fsind = i+1;
                                            break;
                                        }
                                    }

                                    // Write file
                                    struct stat info;
                                    string pathname = (my_dir + "Downloaded");

                                    if(stat(pathname.c_str(), &info) != 0) {
                                        fs::create_directory(pathname);
                                    }
                                    else if(info.st_mode & S_IFDIR) {}

                                    // cout << "Saving the file : " << infile_name[i] << endl;

                                    FILE *fd = fopen((pathname + "/" + infile_name[i]).c_str(), "ab+");

                                    string out(buf+fsind, buf+nbytes);
                                    // cout << "637 : File data received " << out << endl;

                                    fwrite(buf+fsind, sizeof(char), nbytes-fsind, fd);
                                    fclose(fd);

                                    // cout << "642 : Successfully written" << endl;

                                    rem_bytes[i] -= (nbytes-fsind);
                                }
                            }
                            else {
                                // cout << "Currently receiving " << infile_name[i] << " here" << endl;
                                if(rem_bytes[i] > nbytes) {

                                    struct stat info;
                                    string pathname = (my_dir + "Downloaded");

                                    if(stat(pathname.c_str(), &info) != 0) {
                                        fs::create_directory(pathname);
                                    }
                                    else if(info.st_mode & S_IFDIR) {}

                                    FILE *fd = fopen((pathname + "/" + infile_name[i]).c_str(), "ab+");

                                    string out(buf, buf+nbytes);
                                    // cout << "661 : File data received " << out << endl;

                                    fwrite(buf, sizeof(char), nbytes, fd);
                                    fclose(fd);

                                    // cout << "666 : Successfully written" << endl;
                                    rem_bytes[i] -= nbytes;
                                }
                                else {
                                    struct stat info;
                                    string pathname = (my_dir + "Downloaded");

                                    if(stat(pathname.c_str(), &info) != 0) {
                                        fs::create_directory(pathname);
                                    }
                                    else if(info.st_mode & S_IFDIR) {}

                                    FILE *fd = fopen((pathname + "/" + infile_name[i]).c_str(), "ab+");

                                    // fwrite(buf, sizeof(char), eof+1, fd);
                                    
                                    // string out(buf, buf+eof+1);
                                    string out(buf, buf+rem_bytes[i]);
                                    // cout << "684 : File data received " << out << endl;

                                    fwrite(buf, sizeof(char), rem_bytes[i], fd);
                                    fclose(fd);

                                    if(nbytes > rem_bytes[i]){
                                        string remainder(buf+rem_bytes[i], buf+nbytes-2);
                                        // cout << "The remaining part of the message is : " << remainder << endl;

                                        
                                        // It would definitely be a file request
                                        vector<string> fwords = splitstring(remainder);

                                        int n_uid = stoi(fwords[1]), n_id = stoi(fwords[2]);
                                        string fn = fwords[3];

                                        uintmax_t fsz = fs::file_size(my_dir + fn);
                                        
                                        string message = "FILENEXT," + fn + "," + to_string(fsz) + '$';

                                        if(send(i, message.c_str(), message.length()+1, 0) == -1) {
                                            perror("send");
                                        }

                                        FILE *fd = fopen((my_dir + fn).c_str(), "rb");

                                        char file_data[128];

                                        size_t num_bytes = 0;
                                        while ( (num_bytes = fread(file_data, sizeof(char), 127, fd)) > 0)
                                        {
                                            int offset = 0, sent;

                                            while ((sent = send(i, file_data + offset, num_bytes, 0)) > 0
                                                || (sent == -1 && errno == EINTR) ) {
                                                    if (sent > 0) {
                                                        offset += sent;
                                                        num_bytes -= sent;
                                                    }
                                            }
                                        }
                                    }

                                    // cout << "689 : Successfully written" << endl;
                                    file_in[i] = false;

                                    rem_bytes[i] = 0;
                                    files_received++;

                                    // cout << "Analysing the complete message : " << message << endl;

                                    // cout << infile_name[i] << " received successfully and Files received incremented to " << files_received << endl;

                                    if(!sockfd2filereq[i].empty()) {
                                        string req = sockfd2filereq[i].front();
                                        sockfd2filereq[i].pop();

                                        // cout << "Trying to send : " << req << endl;
                                        if(send(i, req.c_str(), req.length()+1, 0) == -1) {
                                            perror("send");
                                            // cout << "Send error" << endl;
                                        }
                                        // else {
                                        //     cout << "Send request for another file : " << req << endl;
                                        // }
                                    }

                                    // Calculate and store the MD5 hash here
                                    unsigned char hash[MD5_DIGEST_LENGTH];
                                    string filename = (pathname + "/" + infile_name[i]);
                                    string result = "";

                                    FILE *inFile = fopen(filename.c_str(), "rb");
                                    MD5_CTX mdContext;
                                    int rbytes;
                                    unsigned char data[1024];
                                    MD5_Init(&mdContext);

                                    while((rbytes = fread(data, 1, 1024, inFile)) != 0)
                                        MD5_Update(&mdContext, data, rbytes);
                                    MD5_Final(hash, &mdContext);
                                    char buf[2*MD5_DIGEST_LENGTH];
                                    for(int i = 0; i < MD5_DIGEST_LENGTH; i++) {
                                        sprintf(buf, "%02x", hash[i]);
                                        result.append(buf);
                                    }
                                    fclose(inFile);

                                    file_hash[infile_name[i]] = result;
                                }
                            }
                        }
                    }
                }
            
                // if(all_requested && all_responded && all_2responded && (d2sent == num_neighbors*(num_neighbors-1)) && !printed) break;
            }

            // if(all_requested && all_responded && all_2responded && (d2sent == num_neighbors*(num_neighbors-1)) && !printed) {
            if(all_requested && all_responded && (received_2REQs == expected_2REQs) && (received_2RESPs == expected_2RESPs) && (received_DRESPs == expected_DRESPs) && !printed) {
                // cout << "Here" << endl;

                for(auto neighbor : neighbors) {
                    int n_id = neighbor.first;
                    int n_port = neighbor.second;
                    int n_uid = get<2>(neighbor_info[n_id]);

                    cout << "Connected to " << n_id << " with unique-ID " << n_uid << " on port " << n_port << endl;
                }

                for(auto file : search_files) {
                    int d = 1, n_uid;
                    if(file_owners[file].empty() && d2file_owners[file].empty()) {
                        d = 0;
                        n_uid = 0;
                        file_hash[file] = "0";
                        file_depth[file] = to_string(d);
                        file_owneruid[file] = to_string(n_uid);
                    }
                    else if(!file_owners[file].empty()) {
                        files_needed++;
                        wanted_files.insert(file);
                        auto it = min_element(file_owners[file].begin(), file_owners[file].end());
                        n_uid = *it;

                        // Send the request here
                        string message = "FILEREQ," + to_string(uid) + "," + to_string(id) + "," + file + '$';

                        // cout << "Will send on " << uid2sockfd[n_uid] << endl;
                        // if(send(uid2sockfd[n_uid], message.c_str(), message.length()+1, 0) == -1) {
                            // perror("send");
                        // }

                        sockfd2filereq[uid2sockfd[n_uid]].push(message);

                        file_depth[file] = to_string(d);
                        file_owneruid[file] = to_string(n_uid);
                    }
                    else {
                        files_needed++;
                        wanted_files.insert(file);
                        d = 2;
                        auto it = min_element(d2file_owners[file].begin(), d2file_owners[file].end());
                        n_uid = get<0>(*it);

                        if(!d2connected.count(n_uid)) {
                            // Connect first
                            if((rv = getaddrinfo(NULL, to_string(get<2>(*it)).c_str(), &hints, &servinfo)) != 0) {
                                // fprintf(stderr, "getaddrinfo: %s$", gai_strerror(rv));
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

                                d2connected.insert(n_uid);
                                d2_uid2outsockfd[n_uid] = client_sockfd;
                                d2_outsockfd2id[client_sockfd] = get<1>(*it);
                                string message = "FILEREQ," + to_string(uid) + "," + to_string(id) + "," + file + '$';
                                // cout << "The socketfd is " << client_sockfd << endl;
                                
                                // if(send(client_sockfd, message.c_str(), message.length()+1, 0) == -1) {
                                //     perror("send");
                                //     // cout << "Error in line 624" << endl;
                                // }

                                sockfd2filereq[d2_uid2outsockfd[n_uid]].push(message);

                                // cout << "Connected to " << n_uid << " and sent : " << message << endl;

                                fdmax = max(fdmax, client_sockfd);
                                FD_SET(client_sockfd, &master);
                                break;
                            }
                            if(p == NULL) {
                                // fprintf(stderr, "client: failed to connect to %d$", neighbors[i].first);
                            }

                            freeaddrinfo(servinfo);
                        }
                        else {
                            string message = "FILEREQ," + to_string(uid) + "," + to_string(id) + "," + file + '$';
                            // cout << "Requesting file on connected depth 2 neighbor on the socket " << d2_uid2outsockfd[n_uid] << endl;
                            // if(send(d2_uid2outsockfd[n_uid], message.c_str(), message.length()+1, 0) == -1) {
                            //     perror("send");
                            // }

                            sockfd2filereq[d2_uid2outsockfd[n_uid]].push(message);
                        }
                        file_depth[file] = to_string(d);
                        file_owneruid[file] = to_string(n_uid);
                    }

                    // string out = "Found " + file + " at " + to_string(n_uid) + " with MD5 0 at depth " + to_string(d);
                    // cout << out << endl;
                }
                printed = true;
                // break;

                // cout << "Files needed for ID : " << id << " is " << files_needed << endl;

                for(int sock = 0; sock <= fdmax; sock++) {
                    if(!sockfd2filereq[sock].empty()) {
                        string req = sockfd2filereq[sock].front();
                        sockfd2filereq[sock].pop();
                        
                        if(send(sock, req.c_str(), req.length()+1, 0) == -1) {
                            // perror("send");
                        }
                    }
                }

                // If at this point files needed is zero then just sort everyone and print the default message here else do the printing
                // in the next block
                if(files_needed == 0) {
                    for(auto file : search_files) {
                        string out = "Found " + file + " at 0 with MD5 0 at depth 0";
                        cout << out << endl;
                    }
                }
            }
        
            // if(all_requested && all_responded && all_2responded && (d2sent == num_neighbors*(num_neighbors-1)) && printed && (files_needed == files_received) && (files_needed != 0) && !hashed) {
            if(all_requested && all_responded && (received_2REQs == expected_2REQs) && (received_2RESPs == expected_2RESPs) && (received_DRESPs == expected_DRESPs) && printed && (files_needed == files_received) && (files_needed != 0) && !hashed) {

                // cout << "Final results" << endl;
                cout << "";
                for(auto file : search_files) {
                    string out = "Found " + file + " at " + file_owneruid[file] + " with MD5 " + file_hash[file] + " at depth " + file_depth[file];
                    std::cout << out << endl;
                }
                hashed = true;
            }
        }
    }
    else {
        for(auto file : search_files) {
            string out = "Found " + file + " at 0 with MD5 0 at depth 0";
            cout << out << endl;
        }
    }

    return 0;
}