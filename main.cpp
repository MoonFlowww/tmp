#include <databento/historical.hpp>
#include <databento/record.hpp>
#include <databento/enums.hpp>
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

    databento::Historical historical(api_key);

    auto symbols = historical.Symbology.Resolve(
        "GLBX.MDP3",
        {"MNQ.CME"},
        databento::SymbologyResolution::Parent,
        "2024-07-28",
        "2025-07-28"
    );

    std::cout << "Contrats trouvés :" << std::endl;
    for (const auto& s : symbols.results) {
        std::cout << "- " << s.symbol << std::endl;
    }

    return 0;
}
