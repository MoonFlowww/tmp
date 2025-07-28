#include <algorithm>
#include <chrono>
#include <databento/datetime.hpp>
#include <databento/historical.hpp>
#include <databento/symbol_map.hpp>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <vector>

std::string GetDateISO8601(int offset_days = 0) {
    using namespace std::chrono;
    auto today = floor<days>(system_clock::now());
    auto target_date = today + days(offset_days);
    std::time_t tt = system_clock::to_time_t(target_date);
    std::tm tm = *std::gmtime(&tt);

    char buffer[11];
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", &tm);
    return std::string(buffer);
}

int main() {
    const char* api_key_env = std::getenv("DATABENTO_API_KEY");
    if (!api_key_env) {
        std::cerr << "Erreur : variable d'environnement DATABENTO_API_KEY non définie !" << std::endl;
        return 1;
    }

    std::string asset;
    std::string asset_type;
    std::cout << "Entrez l'asset (ex: MNQ) : ";
    std::cin >> asset;
    std::cout << "Entrez l'asset type (ex: FUT) : ";
    std::cin >> asset_type;

    std::string symbol_request = asset + "." + asset_type;

    std::string end_date = GetDateISO8601(0);   // today
    std::string start_date = GetDateISO8601(-365);//today - 1yr

    std::cout << "Période utilisée : " << start_date << " -> " << end_date << "\n";

    databento::Historical client = databento::HistoricalBuilder{}
                                       .SetKey(api_key_env)
                                       .Build();

    databento::TsSymbolMap symbol_mappings;
    auto extract_symbol_mappings = [&symbol_mappings](databento::Metadata&& metadata) {
        symbol_mappings = metadata.CreateSymbolMap();
    };

    std::vector<databento::InstrumentDefMsg> definitions;
    std::unordered_set<std::string> seen_symbols;

    auto print_definitions = [&definitions, &seen_symbols](const databento::Record& record) {
        auto def = record.Get<databento::InstrumentDefMsg>();
        std::string symbol(def.RawSymbol());
        if (seen_symbols.insert(symbol).second) {
            definitions.emplace_back(def);
        }
        return databento::KeepGoing::Continue;
    };

    client.TimeseriesGetRange(
        "GLBX.MDP3",
        databento::DateTimeRange<std::string>{start_date, end_date},
        {symbol_request},
        databento::Schema::Definition,
        databento::SType::Parent,
        databento::SType::InstrumentId,
        {},
        extract_symbol_mappings,
        print_definitions);

    std::sort(
        definitions.begin(), definitions.end(),
        [](const databento::InstrumentDefMsg& lhs, const databento::InstrumentDefMsg& rhs) {
            return std::tuple{lhs.expiration, std::string_view{lhs.RawSymbol()}} <
                   std::tuple{rhs.expiration, std::string_view{rhs.RawSymbol()}};
        });

    for (const auto& definition : definitions) {
        std::cout << definition.hd.instrument_id << '\t'
                  << std::setw(9) << symbol_mappings.At(definition) << '\t'
                  << databento::ToIso8601(definition.expiration) << '\n';
    }

    return 0;
}
