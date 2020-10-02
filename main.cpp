#include <array>
#include <algorithm>
#include <iostream>
#include <string_view>
#include <fstream>
#include <cassert>
#include <vector>
#include <sstream>
#include <map>

class ClusterInfo {
public:
  explicit ClusterInfo(uint32_t cnt_servers) noexcept
    : cnt_servers_(cnt_servers)
  {}

  [[nodiscard]]
  auto cnt_servers() const {
    return cnt_servers_;
  }

  // ip:port of other servers in cluster
  template<class Object>
  void send_object(const Object &object, const std::string &prefix) const {
    auto server_id = get_cluster_id(object.id());
    auto server_url = prefix + "." + std::to_string(server_id) + ".csv";

    auto stream_it = streams_cache.find(server_url);
    if (stream_it == streams_cache.end()) {
      stream_it = streams_cache.emplace(server_url, std::ofstream(server_url, std::ios::out | std::ios::app)).first;
    }
    stream_it->second << object << "\n";
  }

  void flush() {
    streams_cache.clear();
  }

  uint64_t get_cluster_id(std::string_view id) const {
    auto hash_of_donor_id = std::hash<std::string_view>{}(id);
    return hash_of_donor_id % cnt_servers();
  }

private:
  mutable std::map<std::string, std::ofstream> streams_cache;
  // got from cluster config
  uint32_t cnt_servers_{5};
};

ClusterInfo cluster_of_collectors{5};
ClusterInfo cluster_of_processors{5};
ClusterInfo cluster_of_aggregators{1};

struct Donor {
  std::string id_;
  std::string state_;
  bool is_processed_;

  explicit Donor(bool is_processed = false)
    : is_processed_(is_processed)
  {}

  [[nodiscard]]
  const std::string &id() const {
    return id_;
  }

  friend std::istream& operator>>(std::istream &in, Donor &donor) {
    if (donor.is_processed_) {
      std::getline(in, donor.id_, ',');
      return std::getline(in, donor.state_);
    }
    std::array<char, 1000> temp_buf{};

    // ID,City,State,Rest part...\n
    std::getline(in, donor.id_, ',');
    in.getline(temp_buf.data(), temp_buf.size(), ','); // skip city
    std::getline(in, donor.state_, ',');
    return in.getline(temp_buf.data(), temp_buf.size()); // skip rest part
  }

  friend std::ostream& operator<<(std::ostream &out, const Donor &donor) {
    return out << donor.id() << "," << donor.state_;
  }

  bool operator<(const Donor& rhs) const {
    return id() < rhs.id();
  }

  explicit operator bool() const {
    return !state_.empty();
  }
};

struct Donation {
  std::string donor_id_;
  std::string amount_;
  bool is_processed_;

  explicit Donation(bool is_processed = false)
    : is_processed_(is_processed)
  {}

  [[nodiscard]]
  const std::string &id() const {
    return donor_id_;
  }

  friend std::istream& operator>>(std::istream &in, Donation &donation) {
    if (donation.is_processed_) {
      std::getline(in, donation.donor_id_, ',');
      return std::getline(in, donation.amount_);
    }
    std::array<char, 1000> temp_buf{};
    // ..,..,Donor ID,...,Donation Amount,...
    in.getline(temp_buf.data(), temp_buf.size(), ','), // skip Project Id
    in.getline(temp_buf.data(), temp_buf.size(), ','); // skip Donation Id
    std::getline(in, donation.donor_id_, ',');
    in.getline(temp_buf.data(), temp_buf.size(), ','); // Donation Included
    std::getline(in, donation.amount_, ',');
    return in.getline(temp_buf.data(), temp_buf.size()); // skip rest part
  }

  friend std::ostream& operator<<(std::ostream &out, const Donation &donation) {
    return out << donation.id() << "," << donation.amount_;
  }

  bool operator<(const Donation& rhs) const {
    return id() < rhs.id();
  }

  explicit operator bool() const {
    return !id().empty();
  }
};

struct StateDonation {
  std::string state_;
  std::string amount_;

  [[nodiscard]]
  const std::string &id() const {
    return state_;
  }

  friend std::istream& operator>>(std::istream &in, StateDonation &state_donation) {
    std::getline(in, state_donation.state_, ',');
    return std::getline(in, state_donation.amount_);
  }

  friend std::ostream& operator<<(std::ostream &out, const StateDonation &state_donation) {
    return out << state_donation.state_ << "," << state_donation.amount_;
  }

  explicit operator bool() const {
    return !state_.empty() && !amount_.empty();
  }
};

namespace {

template<class ResType>
std::vector<ResType> parse_csv(std::string csv_part) {
  if (csv_part.empty()) return {};

  std::stringstream ss(csv_part);
  std::vector<ResType> objects;
  ResType object;
  while (ss >> object) {
    if (object) {
      objects.emplace_back(object);
    }
  }

  return objects;
}

std::string read_whole_file(std::string_view file_name) {
  std::ifstream s(file_name.data());
  return {(std::istreambuf_iterator<char>(s)), std::istreambuf_iterator<char>()};
}

} // namespace

class DonatesCollector {
public:
  explicit DonatesCollector(uint32_t cur_server_id, std::string_view donors_csv = "donors.csv", std::string_view donations_csv = "donations.csv")
    : cur_server_id_(cur_server_id)
    , donors_csv(donors_csv)
    , donations_csv(donations_csv)
  {
    assert(cur_server_id_ < cluster_of_collectors.cnt_servers());
  }

  int run() {
    std::string donors_csv_part = get_my_part_of_csv(donors_csv);
    std::string donations_csv_part = get_my_part_of_csv(donations_csv);

    auto donors = parse_csv<Donor>(std::move(donors_csv_part));
    auto donations = parse_csv<Donation>(std::move(donations_csv_part));

    send_to_processors(std::move(donors), "donors");
    send_to_processors(std::move(donations), "donations");
    cluster_of_processors.flush();

    return 0;
  }

private:

  static auto filesize(std::string_view filename) {
    std::ifstream in(filename.data(), std::ifstream::ate | std::ifstream::binary);
    return in.tellg();
  }

  template<class Object>
  void send_to_processors(std::vector<Object> objects, const std::string &file_prefix) {
    // it's not an optimal solution just for example
    for (const auto &object : objects) {
      cluster_of_processors.send_object(object, "processor_" + file_prefix);
    }
  }

  /**
   * read bytes from file
   * after first \n from position (server_id * (file_size / cnt_servers))
   * before first \n to position (server_id + 1) * (file_size / cnt_servers)
   */
  std::string get_my_part_of_csv(std::string_view filename, uint32_t max_line_length = 200) {
    uint32_t size_in_bytes = filesize(filename);
    // it must be greater than any line in csv file, we assume that every line is <= max_line_length bytes
    auto bytes_per_server = std::max(size_in_bytes / cluster_of_collectors.cnt_servers(), max_line_length);
    auto from = cur_server_id_ * bytes_per_server;

    // length of line is definitely less than 1000 bytes
    std::array<char, 1000> temp_buffer{};
    std::ifstream in(filename.data(), std::ifstream::binary);
    in.seekg(from);
    in.getline(temp_buffer.data(), temp_buffer.size()); // read till \n, and put file pointer after it
    bytes_per_server -= in.gcount();

    std::string part_of_csv(bytes_per_server, '\0');
    in.read(part_of_csv.data(), bytes_per_server);
    part_of_csv.resize(in.gcount());

    if (!in.eof()) {
      in.getline(temp_buffer.data(), temp_buffer.size()); // read rest bytes till \n
      part_of_csv.append(temp_buffer.data(), in.gcount());
    }

    return part_of_csv;
  }

private:
  std::string_view donors_csv;
  std::string_view donations_csv;

  // id of current server in cluster, passed through a cli parameter
  uint32_t cur_server_id_{2};
};

class DonatesProcessor {
public:
  explicit DonatesProcessor(uint32_t cur_server_id)
    : cur_server_id_(cur_server_id) {
  }

  int run() {
    struct ProcessedDonor : Donor {
      ProcessedDonor() : Donor(true) {}
    };
    struct ProcessedDonation : Donation {
      ProcessedDonation() : Donation(true) {}
    };
    auto donors = parse_csv<ProcessedDonor>(read_whole_file("processor_donors." + std::to_string(cur_server_id_) + ".csv"));
    auto donations = parse_csv<ProcessedDonation>(read_whole_file("processor_donations." + std::to_string(cur_server_id_) + ".csv"));

    std::sort(donors.begin(), donors.end());
    std::sort(donations.begin(), donations.end());

    auto donation_it = donations.begin();
    for (const auto &donor : donors) {
      if (donation_it == donations.end()) {
        cluster_of_aggregators.send_object(StateDonation{donor.state_, "0"}, "aggregator");
      }

      // skip donations witout donar
      for (; donation_it != donations.end() && donation_it->id() < donor.id(); ++donation_it);

      for (; donation_it != donations.end() && donation_it->id() == donor.id(); ++donation_it) {
        cluster_of_aggregators.send_object(StateDonation{donor.state_, donation_it->amount_}, "aggregator");
      }
    }
    cluster_of_aggregators.flush();

    return 0;
  }

private:
  uint32_t cur_server_id_;
};

class DonatesAggregator {
public:
  explicit DonatesAggregator(uint32_t cur_server_id)
    : cur_server_id_(cur_server_id) {
  }

  int run() {
    auto state_donations = parse_csv<StateDonation>(read_whole_file("aggregator." + std::to_string(cur_server_id_) + ".csv"));

    std::map<std::string, uint64_t> state_to_donation;
    for (auto &state_donation : state_donations) {
      state_to_donation[state_donation.state_] += std::stoi(state_donation.amount_);
    }

    for (auto &[state, donation] : state_to_donation) {
      std::cout << state << " " << donation << std::endl;
    }

    return 0;
  }

private:
  uint32_t cur_server_id_;
};

void simple_test() {
   for (uint32_t i = 0; i < cluster_of_collectors.cnt_servers(); ++i) {
     DonatesCollector{i}.run();
   }
   for (uint32_t i = 0; i < cluster_of_processors.cnt_servers(); ++i) {
     DonatesProcessor{i}.run();
   }
   for (uint32_t i = 0; i < cluster_of_aggregators.cnt_servers(); ++i) {
     DonatesAggregator{i}.run();
   }
}

int main(int argc, char *args[]) {
  simple_test();
  return 0;
  if (args[1][0] == '0') {
    DonatesCollector{2}.run();
  } else if (args[1][0] == '1') {
    DonatesProcessor{2}.run();
  } else if (args[1][0] == '2') {
    DonatesAggregator{0}.run();
  }
  return 0;

  assert(argc >= 3);
  uint32_t cur_server_id = std::atoi(args[2]);

  if (std::string_view{args[1]} == "collector_mode") {
    return DonatesCollector{cur_server_id}.run();
  }

  if (std::string_view{args[1]} == "processor_mode") {
    return DonatesProcessor{cur_server_id}.run();
  }

  if (std::string_view{args[1]} == "aggregator_mode") {
    return DonatesAggregator{cur_server_id}.run();
  }

  assert(false);
}
