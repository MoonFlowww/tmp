#include <databento/historical.hpp>
#include <iostream>
#include <cstdlib>
#include <string>

int main() {
    const char* api_key_env = std::getenv("DATABENTO_API_KEY");
    if (!api_key_env) {
        std::cerr << "Erreur : variable d'environnement DATABENTO_API_KEY non définie !" << std::endl;
        return 1;
    }
    std::string api_key(api_key_env);
    std::cout << "Clé API récupérée : " << api_key << std::endl;

    databento::Historical historical(
        nullptr,
        api_key,
        databento::HistoricalGateway::Bo1
    );

    auto symbols = historical.ListSymbols("GLBX.MDP3");

    std::cout << "Contrats disponibles pour MNQ :" << std::endl;
    for (const auto& sym : symbols) {
        if (sym.rfind("MNQ", 0) == 0) {  // Filtrer ceux qui commencent par MNQ
            std::cout << "- " << sym << std::endl;
        }
    }

    return 0;
}
