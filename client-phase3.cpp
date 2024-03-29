// Command to compile
// g++ -std=c++17 -g -Wall -o client-phase3 client-phase3.cpp -lssl -lcrypto

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
            string w = s.substr(lc+1, (i-lc-1));
            out.push_back(w);
            lc = i;
        }
    }

    return out;
}

int main(int argc, char **argv) {
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
    bool all_responded = false, all_requested = false;
    // int all_got = 0;
    bool all_printed = false;

    bool phase1_printed = false;

    int files_expected = 0, files_got = 0;

    // bool file_in = false;
    map<int, bool> file_in;
    // Map from sockfd to bool
    
    // int rem_bytes;
    map<int, int> rem_bytes;
    // Map from sockfd to int

    // string infile_name;
    map<int, string> infile_name;
    // Map from sockfd to string

    map<string, string> file_hash;

    map<int, queue<string>> sockfd2filereq;

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
    memset(&buf, 0, sizeof buf);
    
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

            string message = "INFO," + to_string(id) + "," + to_string(port) + "," + to_string(uid) + '$';
            if(send(client_sockfd, message.c_str(), message.length()+1, 0) == -1) {
                // perror("send");
            }

            message = "REQ," + to_string(uid) + "," + to_string(id) + "," + to_string(num_files_down);

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

                            string message = "INFO," + to_string(id) + "," + to_string(port) + "," + to_string(uid) + '$';
                            if(send(newfd, message.c_str(), message.length()+1, 0) == -1) {
                                // perror("send");
                            }

                            message = "REQ," + to_string(uid) + "," + to_string(id) + "," + to_string(num_files_down);

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
                            // if(id == 4) cout << message << endl;
                            // cout << message << endl;
                            // cout << flush;

                            if(!file_in[i]) {
                                int fsind = 0;
                                vector<string> msgs = splitmsg(message);
                                // This function will be now adapted to fit for both
                                // messages and file data

                                // Loop to clean the messages
                                for(int i = 1; i < msgs.size(); i++) {
                                    if(msgs[i].length())
                                        msgs[i] = msgs[i].substr(1, msgs[i].length()-1);
                                }

                                // chats[sockfd2ID[i]].insert(chats[sockfd2ID[i]].end(), msgs.begin(), msgs.end());
                                
                                for(auto msg : msgs) {
                                    vector<string> words = splitstring(msg);
                                    // cout << msg;

                                    if(words[0] == "INFO") {
                                        int n_id, n_port, n_uid;

                                        // cout << "stoi check : " << words[0] << endl;

                                        n_id = stoi(words[1]);
                                        sockfd2ID[i] = n_id;
                                        n_port = stoi(words[2]);
                                        n_uid = stoi(words[3]);

                                        uid2sockfd[n_uid] = i;
                                        get<0>(neighbor_info[n_id]) = i;
                                        get<2>(neighbor_info[n_id]) = n_uid;
                                    }
                                    else {
                                        if(words[0] == "REQ") {
                                            int n_uid = stoi(words[1]), n_id = stoi(words[2]), num_files = stoi(words[3]);
                                            // cout << "stoi check : " << n_uid << n_id << num_files << endl;

                                            string message = "RESP," + to_string(uid) + "," + to_string(id) +"," + to_string(num_files);

                                            for(int i = 0; i < num_files; i++) {
                                                string fn = words[4+i];

                                                // if(id == 5) cout << "406 " << fn << " " << fn.length() << endl;

                                                if(my_files_set.count(fn)) {
                                                    message += ",YES";
                                                    // cout << "Found " << fn << endl;
                                                }
                                                else {
                                                    message += ",NO";
                                                    // cout << "Not found " << fn << endl;
                                                }
                                            }

                                            message += '$';

                                            if(send(i, message.c_str(), message.length()+1, 0) == -1) {
                                                perror("send");
                                            }

                                            unrequested.erase(n_id);
                                            if(unrequested.empty()) all_requested = true;
                                        }
                                        else if(words[0] == "RESP") {
                                            int n_uid = stoi(words[1]), n_id = stoi(words[2]), num_files = stoi(words[3]);

                                            // cout << "stoi check : " << n_uid << n_id << num_files << endl;

                                            for(int i = 0; i < num_files; i++) {
                                                string resp = words[4+i];

                                                if(resp == "YES") {
                                                    file_owners[search_files[i]].insert(n_uid);
                                                }
                                            }
                                        
                                            unresponded.erase(n_id);
                                            if(unresponded.empty()) {
                                                all_responded = true;

                                                // Ask for the file now
                                                
                                                for(int ind = 0; ind < num_files; ind++) {
                                                    if(!file_owners[search_files[ind]].empty()) {
                                                        string message = "FILEREQ," + to_string(uid) + "," + to_string(id) + "," + search_files[ind];
                                                        message += '$';

                                                        auto it = min_element(file_owners[search_files[ind]].begin(), file_owners[search_files[ind]].end());
                                                        int n_uid = *it;

                                                        // if(send(uid2sockfd[n_uid], message.c_str(), message.length()+1, 0) == -1) {
                                                        //     perror("send");
                                                        // }
                                                        sockfd2filereq[uid2sockfd[n_uid]].push(message);
                                                        files_expected++;

                                                        // cout << "Files expected increased to " << files_expected << endl;
                                                    }
                                                }

                                                for(int ind = 0; ind < num_neighbors; ind++) {
                                                    int sock = get<0>(neighbor_info[neighbors[ind].first]);

                                                    if(!sockfd2filereq[sock].empty()) {
                                                        string req = sockfd2filereq[sock].front();
                                                        sockfd2filereq[sock].pop();

                                                        if(send(sock, req.c_str(), req.length()+1, 0) == -1) {
                                                            perror("send");
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        else if(words[0] == "FILEREQ") {
                                            int n_uid = stoi(words[1]), n_id = stoi(words[2]);

                                            // if(n_id == 4) cout << "4 has requested " << msg << endl;

                                            string fn = words[3];

                                            uintmax_t fsz = fs::file_size(my_dir + fn);
                                            
                                            string message = "FILENEXT," + fn + "," + to_string(fsz) + '$';

                                            if(send(uid2sockfd[n_uid], message.c_str(), message.length()+1, 0) == -1) {
                                                perror("send");
                                            }

                                            FILE *fd = fopen((my_dir + fn).c_str(), "rb");

                                            char file_data[128];

                                            size_t num_bytes = 0;
                                            while ( (num_bytes = fread(file_data, sizeof(char), 127, fd)) > 0)
                                            {
                                                int offset = 0, sent;
                                                while ((sent = send(uid2sockfd[n_uid], file_data + offset, num_bytes, 0)) > 0
                                                    || (sent == -1 && errno == EINTR) ) {
                                                        if (sent > 0) {
                                                            offset += sent;
                                                            num_bytes -= sent;
                                                        }
                                                }
                                            }
                                        }
                                        else if(words[0] == "FILENEXT") {
                                            file_in[i] = true;
                                            rem_bytes[i] = stoi(words[2]);
                                            infile_name[i] = words[1];
                                            // all_got++;
                                            // files_got++;
                                            // cout << "Files got increased to " << files_got << endl;
                                            break;
                                        }
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

                                    FILE *fd = fopen((pathname + "/" + infile_name[i]).c_str(), "ab+");

                                    string out(buf+fsind, buf+nbytes);
                                    // cout << out;

                                    fwrite(buf+fsind, sizeof(char), nbytes-fsind, fd);
                                    fclose(fd);

                                    rem_bytes[i] -= (nbytes-fsind);
                                }
                            }
                            else {
                                if(rem_bytes[i] > nbytes) {

                                    struct stat info;
                                    string pathname = (my_dir + "Downloaded");

                                    if(stat(pathname.c_str(), &info) != 0) {
                                        fs::create_directory(pathname);
                                    }
                                    else if(info.st_mode & S_IFDIR) {}

                                    FILE *fd = fopen((pathname + "/" + infile_name[i]).c_str(), "ab+");

                                    fwrite(buf, sizeof(char), nbytes, fd);

                                    fclose(fd);
                                    string out(buf, buf+nbytes);
                                    // cout << out;
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

                                    file_in[i] = false;

                                    // string out(buf, buf+eof+1);
                                    string out(buf, buf+rem_bytes[i]);
                                    // cout << out;

                                    // string temp(buf+rem_bytes[i], buf+nbytes);
                                    // cout << "The remaining part of the message is : " << temp << endl;

                                    rem_bytes[i] = 0;


                                    if(!sockfd2filereq[i].empty()) {
                                        string req = sockfd2filereq[i].front();
                                        sockfd2filereq[i].pop();

                                        // if(id == 4) cout << "Sent the next request : " << req << endl;
                                        cout << "";
                                        // cout.flush();
                                        // cerr.flush();

                                        if(send(i, req.c_str(), req.length()+1, 0) == -1) {
                                            perror("send");
                                        }
                                    }

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
                                    files_got++;
                                }
                            }
                        }
                    }
                }
            
                // if(all_requested && all_responded) {
                //     int cntr = 0;
                //     for(int i = 0; i < num_files_down; i++) {
                //         if(!file_owners[search_files[i]].empty()) cntr++;
                //     }
                //     if(cntr == all_got) break;
                // }
            }

            if(all_requested && all_responded && !phase1_printed) {
                for(auto neighbor : neighbors) {
                    int n_id = neighbor.first;
                    int n_port = neighbor.second;
                    int n_uid = get<2>(neighbor_info[n_id]);

                    cout << "Connected to " << n_id << " with unique-ID " << n_uid << " on port " << n_port << endl;
                }

                phase1_printed = true;
                // cout << "Printed" << endl;
            }

            if(all_requested && all_responded) {
                // int cntr = 0;
                // for(int i = 0; i < num_files_down; i++) {
                //     if(!file_owners[search_files[i]].empty()) cntr++;
                // }
                if(files_expected == files_got) {
                    // break;
                    if(!all_printed) {
                        // Now print all the resultant output

                        for(auto file : search_files) {
                            // cout << "Gonna print" << endl;
                            int d = 1, n_uid;
                            if(file_owners[file].empty()) {
                                d = 0;
                                n_uid = 0;

                                string out = "Found " + file + " at " + to_string(n_uid) + " with MD5 0 at depth " + to_string(d);
                                cout << out << endl;
                            }
                            else {
                                auto it = min_element(file_owners[file].begin(), file_owners[file].end());
                                n_uid = *it;
                            
                                string out1 = "Found " + file + " at " + to_string(n_uid) + " with MD5 ", out2 = " at depth " + to_string(d);
                                cout << out1 << file_hash[file] << out2 << endl;
                            }
                        }

                        all_printed = true;
                    }
                }
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