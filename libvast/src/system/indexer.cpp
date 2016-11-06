

  struct loader {
    loader(event_indexer& ei) : indexer_{ei} {
    }

    template <typename T>
    std::vector<actor> operator()(T const&) {
      return {};
    }

    template <typename T, typename U>
    std::vector<actor> operator()(T const&, U const&) {
      return {};
    }

    std::vector<actor> operator()(predicate const& p) {
      op_ = p.op;
      return visit(*this, p.lhs, p.rhs);
    }

    std::vector<actor> operator()(event_extractor const&, data const&) {
      return {indexer_.spawn_name_indexer()};
    }

    std::vector<actor> operator()(time_extractor const&, data const&) {
      return {indexer_.spawn_time_indexer()};
    }

    std::vector<actor> operator()(type_extractor const& e, data const&) {
      std::vector<actor> indexes;
      if (auto r = get<type::record>(indexer_.type_)) {
        for (auto& i : type::record::each{*r})
          if (i.trace.back()->type == e.type) {
            auto a = indexer_.spawn_data_indexer(i.offset);
            if (a) {
              indexes.push_back(std::move(*a));
            } else {
              VAST_ERROR(a.error());
              return {};
            }
          }
      } else if (indexer_.type_ == e.type) {
        auto a = indexer_.spawn_data_indexer({});
        if (a)
          indexes.push_back(std::move(*a));
        else
          VAST_ERROR(a.error());
      }
      return indexes;
    }

    std::vector<actor> operator()(schema_extractor const& e,
                                       data const& d) {
      std::vector<actor> indexes;
      if (auto r = get<type::record>(indexer_.type_)) {
        for (auto& pair : r->find_suffix(e.key)) {
          auto& o = pair.first;
          auto lhs = r->at(o);
          VAST_ASSERT(lhs);
          if (!compatible(*lhs, op_, type::derive(d))) {
            VAST_WARN("type clash: LHS =", *lhs, "<=> RHS =", type::derive(d));
            return {};
          }
          auto a = indexer_.spawn_data_indexer(o);
          if (!a)
            VAST_ERROR(a.error());
          else
            indexes.push_back(std::move(*a));
        }
      } else if (e.key.size() == 1
                 && pattern::glob(e.key[0]).match(indexer_.type_.name())) {
        auto a = indexer_.spawn_data_indexer({});
        if (a)
          indexes.push_back(std::move(*a));
        else
          VAST_ERROR(a.error());
      }

      return indexes;
    }

    template <typename T>
    std::vector<actor> operator()(data const& d, T const& e) {
      return (*this)(e, d);
    }

    relational_operator op_;
    event_indexer& indexer_;
  };
