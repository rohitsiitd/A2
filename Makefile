# Builds inside the FreeBSD VM via deploy.ps1 (which runs `make` after
# syncing). `c++` is clang++ on FreeBSD.
#
# C++ throughout (instructor-confirmed exception to the handout's C/Python
# language list), using the POSIX socket API directly — no Boost.Asio or
# other networking library, same restriction as for C.
#
# echo_server/echo_client are the learning skeleton (kept as-is); exchange_server
# is the real protocol; trader_client/market_data_client are the two clients
# the launchers in server/ and client/ expect.

CXX      = c++
CXXFLAGS = -Wall -Wextra -std=c++17 -g

.PHONY: all clean

all: echo_server echo_client exchange_server trader_client market_data_client client_generator

echo_server: src/echo_server.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

echo_client: src/echo_client.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

exchange_server: src/exchange_server.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

trader_client: src/trader_client.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

market_data_client: src/market_data_client.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

client_generator: src/client_generator.cpp
	$(CXX) $(CXXFLAGS) -o $@ $<

clean:
	rm -f echo_server echo_client exchange_server trader_client market_data_client client_generator
