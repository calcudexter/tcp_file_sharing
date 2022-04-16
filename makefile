all: client-phase1.cpp client-phase2.cpp client-phase3.cpp client-phase4.cpp client-phase5.cpp
	g++ -std=c++17 client-phase1.cpp -o client-phase1
	g++ -std=c++17 client-phase2.cpp -o client-phase2
	g++ -g -std=c++17 client-phase3.cpp -o client-phase3 -lssl -lcrypto
	g++ -std=c++17 client-phase4.cpp -o client-phase4
	g++ -g -std=c++17 client-phase5.cpp -o client-phase5 -lssl -lcrypto