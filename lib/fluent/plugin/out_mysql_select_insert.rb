#
# Copyright 2018- abicky
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require "fluent/plugin/output"
require "mysql2"

module Fluent
  module Plugin
    class MysqlSelectInsertOutput < Output
      Fluent::Plugin.register_output("mysql_select_insert", self)

      config_param :host, :string, default: "127.0.0.1",
        desc: "Database host"
      config_param :port, :integer, default: 3306,
        desc: "Database port"
      config_param :database, :string,
        desc: "Database name"
      config_param :username, :string,
        desc: "Database user"
      config_param :password, :string, default: "", secret: true,
        desc: "Database password"
      config_param :table, :string,
        desc: "Table into which records are inserted"

      config_param :select_query, :string,
        desc: "SELECT query without WHERE clause to insert records"
      config_param :condition_column, :string,
        desc: "Column used in WHERE clause to compare with values of 'condition_key' in events"
      config_param :condition_key, :string,
        desc: "Key whose value is used in WHERE clause to compare with 'condition_column'"
      config_param :extra_condition, :array, value_type: :string, default: [],
        desc: "Extra condition used in WHERE clause. You can specify placeholders like 'col1 = ? AND col2 = ?, ${tag[0]}, ${tag[1]}'. The metadata is extracted in the second or following arguments."
      config_param :inserted_columns, :array, value_type: :string, default: nil,
        desc: "Columns to be inserted. You should insert all columns by the select query if you don't specify the value."

      config_param :ignore, :bool, default: false,
        desc: "Add 'IGNORE' modifier to INSERT statement"

      config_section :buffer do
        config_set_default :chunk_limit_records, 1000
      end

      def configure(conf)
        super

        if select_query =~ /\bwhere\b/i
          fail Fluent::ConfigError, "You can't specify WHERE clause in 'select_query'"
        end
        if select_query !~ /\A\s*select\b/i
          fail Fluent::ConfigError, "'select_query' should begin with 'SELECT'"
        end
      end

      def write(chunk)
        client = new_client
        condition_values = []
        chunk.msgpack_each do |time, record|
          condition_values << "'#{client.escape(record[condition_key])}'"
        end

        sql = <<~SQL
          INSERT #{"IGNORE" if ignore} INTO `#{table}` #{"(#{inserted_columns.join(",")})" if inserted_columns}
          #{select_query}
          WHERE #{condition_column} IN (#{condition_values.join(",")})
        SQL
        sql << " AND (#{extra_condition[0]})" unless extra_condition.empty?

        bound_params = extra_condition[1..-1]&.map { |c| extract_placeholders(c, chunk.metadata) }
        begin
          if bound_params.nil? || bound_params.empty?
            client.query(sql)
          else
            require "mysql2-cs-bind"
            client.xquery(sql, bound_params)
          end
        rescue Mysql2::Error => e
          if e.message.start_with?("Column count doesn't match value count")
            raise Fluent::UnrecoverableError, "#{e.class}: #{e}"
          end
          raise
        end

        client.close
      end

      def multi_workers_ready?
        true
      end

      private

      def new_client
        Mysql2::Client.new(
          host: @host,
          port: @port,
          username: @username,
          password: @password,
          database: @database,
        )
      end
    end
  end
end
