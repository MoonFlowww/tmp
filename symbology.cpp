#include <algorithm>
#include <databento/datetime.hpp>
#include <databento/historical.hpp>
#include <databento/symbol_map.hpp>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>

using namespace databento;

int main() {
    TsSymbolMap symbol_mappings;
    auto extract_symbol_mappings = [&symbol_mappings](Metadata&& metadata) {
        symbol_mappings = metadata.CreateSymbolMap();
    };

    std::vector<InstrumentDefMsg> definitions;
    auto print_definitions = [&definitions](const Record& record) {
        definitions.emplace_back(record.Get<InstrumentDefMsg>());
        return KeepGoing::Continue;
    };

    const char* api_key_env = std::getenv("DATABENTO_API_KEY");
    if (!api_key_env) {
        std::cerr << "Erreur : variable d'environnement DATABENTO_API_KEY non définie !" << std::endl;
        return 1;
    }

    auto client = HistoricalBuilder{}
                      .SetKey(api_key_env)
                      .Build();

    client.TimeseriesGetRange(
        "GLBX.MDP3",
        DateTimeRange<std::string>{"2024-07-28", "2025-07-28"}, 
        {"MNQ.FUT"},
        Schema::Definition,
        SType::Parent,
        SType::InstrumentId,
        {},
        extract_symbol_mappings,
        print_definitions);

    std::sort(
        definitions.begin(), definitions.end(),
        [](const auto& lhs, const auto& rhs) {
            return std::tuple{lhs.expiration, std::string_view{lhs.RawSymbol()}} <
                   std::tuple{rhs.expiration, std::string_view{rhs.RawSymbol()}};
        });

    for (const auto& definition : definitions) {
        std::cout << definition.hd.instrument_id << '\t'
                  << std::setw(9) << symbol_mappings.At(definition) << '\t'
                  << ToIso8601(definition.expiration) << '\n';
    }

    return 0;
}
