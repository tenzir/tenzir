#include "vast/meta/taxonomy.h"

#include "vast/fs/fstream.h"
#include "vast/meta/ast.h"
#include "vast/meta/parser/taxonomy.h"
#include "vast/meta/argument.h"
#include "vast/meta/event.h"
#include "vast/meta/exception.h"
#include "vast/meta/type.h"
#include "vast/util/parser/parse.h"
#include "vast/util/logger.h"

namespace vast {
namespace meta {

/// Creates an argument from an argument declaration.
static argument_ptr make_arg(std::vector<type_ptr> const& types,
                             ast::argument_declaration const& arg_decl);

/// Creates a meta::type from a taxonomy type in the taxonomy AST.
class type_creator
{
public:
    typedef type_ptr result_type;

    type_creator(std::vector<type_ptr> const& types)
      : types_(types)
    {
    }

    type_ptr operator()(ast::type const& type) const
    {
        return boost::apply_visitor(*this, type);
    }

    type_ptr operator()(ast::type_info const& info) const
    {
        return boost::apply_visitor(*this, info);
    }

    type_ptr operator()(std::string const& str) const
    {
        auto i = std::find_if(
            types_.begin(),
            types_.end(),
            [&str](type_ptr const& t)
            {
                return t->name() == str;
            });

        assert(i != types_.end());
        return *i;
    }

    type_ptr operator()(ast::basic_type const& t) const
    {
        switch (t)
        {
            default:
                assert(! "missing type implementation");
                break;
            case ast::bool_type:
                return new bool_type;
            case ast::int_type:
                return new int_type;
            case ast::uint_type:
                return new uint_type;
            case ast::double_type:
                return new double_type;
            case ast::duration_type:
                return new duration_type;
            case ast::timepoint_type:
                return new timepoint_type;
            case ast::string_type:
                return new string_type;
            case ast::regex_type:
                return new regex_type;
            case ast::address_type:
                return new address_type;
            case ast::prefix_type:
                return new prefix_type;
            case ast::port_type:
                return new port_type;
        }
    }

    type_ptr operator()(ast::enum_type const& t) const
    {
        auto e = new enum_type;
        e->fields = t.fields;

        return e;
    }

    type_ptr operator()(ast::vector_type const& t) const
    {
        auto vector = new vector_type;
        vector->elem_type = boost::apply_visitor(*this, t.element_type);

        return vector;
    }

    type_ptr operator()(ast::set_type const& t) const
    {
        auto set = new set_type;
        set->elem_type = boost::apply_visitor(*this, t.element_type);

        return set;
    }

    type_ptr operator()(ast::table_type const& t) const
    {
        auto table = new table_type;
        table->key_type = boost::apply_visitor(*this, t.key_type);
        table->value_type = boost::apply_visitor(*this, t.value_type);

        return table;
    }

    type_ptr operator()(ast::record_type const& t) const
    {
        auto record = new record_type;
        for (auto& arg : t.args)
            record->args.emplace_back(make_arg(types_, arg));

        return record;
    }

private:
    std::vector<type_ptr> const& types_;
};

static argument_ptr make_arg(
        std::vector<type_ptr> const& types,
        ast::argument_declaration const& arg_decl)
{
    type_ptr t = boost::apply_visitor(type_creator(types), arg_decl.type);
    argument_ptr arg = new argument(arg_decl.name, t);

    // TODO: parse attributes and change argument accordingly.
    if (arg_decl.attrs)
    {
    }

    return arg;
}

/// Generates types and events from the taxonomy AST.
class generator
{
public:
    typedef void result_type;

    generator(std::vector<type_ptr>& types, std::vector<event_ptr>& events)
      : types_(types)
      , events_(events)
    {
    }

    void operator()(ast::type_declaration const& type_decl) const
    {
        assert(! type_decl.name.empty());

        type_ptr t = boost::apply_visitor(type_creator(types_), type_decl.type);
        t = t->symbolize(type_decl.name);

        LOG(debug, meta)
            << "new type:  " << t->name() << " -> " << t->to_string(true);

        types_.emplace_back(std::move(t));
    }

    void operator()(ast::event_declaration const& event_decl) const
    {
        assert(! event_decl.name.empty());
        auto i = std::find_if(
            events_.begin(),
            events_.end(),
            [&event_decl](event_ptr const& e)
            {
                return e->name() == event_decl.name;
            });
        assert(i == events_.end());


        std::vector<argument_ptr> args;
        if (event_decl.args)
            for (auto& arg : *event_decl.args)
                args.emplace_back(make_arg(types_, arg));

        event_ptr e = new event(event_decl.name, args);
        LOG(debug, meta) << "new event: " << *e;
        events_.push_back(std::move(e));
    }

private:
    std::vector<type_ptr>& types_;
    std::vector<event_ptr>& events_;
};

taxonomy::taxonomy()
{
}

taxonomy::~taxonomy()
{
}

void taxonomy::load(std::string const& contents)
{
    LOG(debug, meta) << "parsing taxonomy";
    ast::taxonomy tax_ast;
    if (! util::parser::parse<parser::taxonomy>(contents, tax_ast))
        throw syntax_exception();

    if (tax_ast.empty())
        LOG(warn, meta) << "taxonomy did not contain any statements";

    LOG(debug, meta) << "generating taxonomy";
    generator g(types_, events_);
    for (auto& statement : tax_ast)
        boost::apply_visitor(std::ref(g), statement);
}

void taxonomy::load(const fs::path& filename)
{
    fs::ifstream in(filename);

    std::string storage;
    in.unsetf(std::ios::skipws); // No white space skipping.
    std::copy(std::istream_iterator<char>(in),
            std::istream_iterator<char>(),
            std::back_inserter(storage));

    load(storage);
}

void taxonomy::save(fs::path const& filename) const
{
    fs::ofstream(filename) << to_string();
}

std::string taxonomy::to_string() const
{
    std::stringstream ss;
    for (auto& type : types_)
        ss << "type " << type->name() << ": " << type->to_string(true) << '\n';

    if (! events_.empty())
        ss << '\n';

    for (auto& event : events_)
        ss << "event " << *event << '\n';

    return ss.str();
}

} // namespace meta
} // namespace vast
