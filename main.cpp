#include <databento/datetime.hpp>
#include <databento/dbn.hpp>
#include <databento/historical.hpp>
#include <iostream>
#include <string>
#include <vector>
#include <cstdlib>

int main() {
    try {
        const char* api_key_env = std::getenv("DATABENTO_API_KEY");
    
        if (api_key_env == nullptr) {
            std::cerr << "Erreur : variable d'environnement DATABENTO_API_KEY non définie !" << std::endl;
            return 1;
        }
        std::string api_key(api_key_env);

        std::cout << "Clé API récupérée : " << api_key << std::endl;

      
        databento::HistoricalClient client{api_key};

        auto metadata = client.Metadata(
            databento::MetadataRequest{
                .dataset = "GLBX.MDP3",
                .symbols = {"MNQ.CME"},      // Continuous symbol
                .start = "2024-07-28",
                .end = "2025-07-28"
            }
        );

        for (const auto& contract : metadata.symbols) {
            std::cout << "Contract: " << contract << std::endl;
        }


    } catch (const std::exception& ex) {
        std::cerr << "Erreur : " << ex.what() << std::endl;
        return 1;
    }

    return 0;
}
