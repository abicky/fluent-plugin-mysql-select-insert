require "helper"
require "fluent/plugin/out_mysql_select_insert"

class MysqlSelectInsertOutputTest < Test::Unit::TestCase
  class << self
    def startup
      client = new_client
      client.query(<<~SQL)
        CREATE TABLE IF NOT EXISTS `devices` (
          `id` int(10) unsigned NOT NULL,
          `app_id` int(10) unsigned NOT NULL,
          `uuid` char(36) NOT NULL,
          PRIMARY KEY (`id`),
          UNIQUE KEY (`app_id`, `uuid`)
        )
      SQL
      client.query(<<~SQL)
        CREATE TABLE IF NOT EXISTS `users` (
          `id` int(10) unsigned NOT NULL,
          `device_id` int(10) unsigned NOT NULL,
          PRIMARY KEY (`id`)
        )
      SQL
      client.query(<<~SQL)
        CREATE TABLE IF NOT EXISTS `accessed_users` (
          `user_id` int(10) unsigned NOT NULL,
          PRIMARY KEY (`user_id`)
        )
      SQL
    end

    def shutdown
      client = new_client
      client.query("DROP TABLE IF EXISTS `devices`")
      client.query("DROP TABLE IF EXISTS `users`")
      client.query("DROP TABLE IF EXISTS `accessed_users`")
    end

    def new_client
      Mysql2::Client.new(
        username: "root",
        database: "fluent_plugin_mysql_select_insert",
      )
    end
  end

  def suppress_errors
    prev_value = Thread.report_on_exception
    # Supporess errors from flush treads
    Thread.report_on_exception = false
    # Suppress errors like "unexpected error while after_shutdown"
    capture_stdout { yield }
  ensure
    Thread.report_on_exception = prev_value
  end

  setup do
    Fluent::Test.setup

    client = self.class.new_client
    client.query(<<~SQL)
      INSERT INTO `devices` (`id`, `app_id`, `uuid`) VALUES
        (1, 1, '03449258-29ce-403c-900a-a2c6ea1d09a2'),
        (2, 1, '11aebc82-b661-44ab-951d-d1618058699a'),
        (3, 2, '2c9a7bb7-aec2-441c-ade0-edf31fdacb43'),
        (4, 2, '3eae0b4b-bdd1-4c82-b04a-5335535f5b7b')
    SQL
    client.query(<<~SQL)
      INSERT INTO `users` (`id`, `device_id`) VALUES
        (1001, 1),
        (1002, 2),
        (1003, 3),
        (1004, 4)
    SQL
  end

  teardown do
    client = self.class.new_client
    client.query("TRUNCATE TABLE `devices`")
    client.query("TRUNCATE TABLE `users`")
    client.query("TRUNCATE TABLE `accessed_users`")
  end

  CONFIG = <<~CONF
    database fluent_plugin_mysql_select_insert
    username root

    table accessed_users

    select_query "SELECT
        users.id
      FROM
        users
        INNER JOIN
          devices
        ON
          devices.id = users.device_id"

    condition_key uuid
    condition_column devices.uuid
  CONF

  test "write" do
    d = create_driver
    d.run(default_tag: "tag") do
      d.feed(event_time, { "uuid" => "03449258-29ce-403c-900a-a2c6ea1d09a2", "app_id" => 1 })
      d.feed(event_time, { "uuid" => "11aebc82-b661-44ab-951d-d1618058699a", "app_id" => 1 })
      d.feed(event_time, { "uuid" => "2c9a7bb7-aec2-441c-ade0-edf31fdacb43", "app_id" => 2 })
    end

    result = self.class.new_client.query('SELECT * FROM `accessed_users`')
    assert_equal [{ "user_id" => 1001 }, { "user_id" => 1002 }, { "user_id" => 1003 }], result.to_a
  end

  sub_test_case "'extra_condition' is specified" do
    test "write" do
      d = create_driver(<<~CONF)
        database fluent_plugin_mysql_select_insert
        username root

        table accessed_users

        select_query "SELECT
            users.id
          FROM
            users
            INNER JOIN
              devices
            ON
              devices.id = users.device_id"

        condition_key uuid
        condition_column devices.uuid
        extra_condition "app_id = 1"

        <buffer>
        </buffer>
      CONF
      d.run(default_tag: "tag") do
        d.feed(event_time, { "uuid" => "03449258-29ce-403c-900a-a2c6ea1d09a2", "app_id" => 1 })
        d.feed(event_time, { "uuid" => "2c9a7bb7-aec2-441c-ade0-edf31fdacb43", "app_id" => 2 })
      end

      result = self.class.new_client.query('SELECT * FROM `accessed_users`')
      assert_equal [{ "user_id" => 1001 }], result.to_a
    end
  end

  sub_test_case "'extra_condition' is specified with placeholders" do
    config = <<~CONF
      database fluent_plugin_mysql_select_insert
      username root

      table accessed_users

      select_query "SELECT
          users.id
        FROM
          users
          INNER JOIN
            devices
          ON
            devices.id = users.device_id"

      condition_key uuid
      condition_column devices.uuid
      extra_condition "app_id = ?, ${app_id}"

      <buffer app_id>
      </buffer>
    CONF

    test "write with valid data" do
      d = create_driver(config)
      d.run(default_tag: "tag") do
        # Correct app_id
        d.feed(event_time, { "uuid" => "03449258-29ce-403c-900a-a2c6ea1d09a2", "app_id" => 1 })
      end
      result = self.class.new_client.query('SELECT * FROM `accessed_users`')
      assert_equal [{ "user_id" => 1001 }], result.to_a
    end

    test "write with invalid data" do
      d = create_driver(config)
      d.run(default_tag: "tag") do
        # Different app_id
        d.feed(event_time, { "uuid" => "03449258-29ce-403c-900a-a2c6ea1d09a2", "app_id" => 2 })
      end
      result = self.class.new_client.query('SELECT * FROM `accessed_users`')
      assert_equal [], result.to_a
    end
  end

  sub_test_case "'ignore' is specified" do
    base_config = <<~CONF
      database fluent_plugin_mysql_select_insert
      username root

      table accessed_users

      select_query "SELECT
          users.id
        FROM
          users
          INNER JOIN
            devices
          ON
            devices.id = users.device_id"

      condition_key uuid
      condition_column devices.uuid
    CONF

    test "write with ignore false" do
      self.class.new_client.query("INSERT INTO `accessed_users` VALUES (1001)")

      d = create_driver("#{base_config}\n ignore false")
      assert_raise Mysql2::Error do
        suppress_errors do
          d.run(default_tag: "tag") do
            d.feed(event_time, { "uuid" => "03449258-29ce-403c-900a-a2c6ea1d09a2", "app_id" => 1 })
          end
        end
      end
    end

    test "write with ignore true" do
      self.class.new_client.query("INSERT INTO `accessed_users` VALUES (1001)")

      d = create_driver("#{base_config}\n ignore true")
      assert_nothing_raised do
        d.run(default_tag: "tag") do
          d.feed(event_time, { "uuid" => "03449258-29ce-403c-900a-a2c6ea1d09a2", "app_id" => 1 })
        end
      end
    end
  end

  sub_test_case "WHERE clause is specified in 'select_query'" do
    test "configure" do
      assert_raise Fluent::ConfigError do
        create_driver(<<~CONF)
          database fluent_plugin_mysql_select_insert
          username root

          table accessed_users

          select_query "SELECT
              users.id
            FROM
              users
              INNER JOIN
                devices
              ON
                devices.id = users.device_id
            WHERE
              app_id = 1"

          condition_key uuid
          condition_column devices.uuid
        CONF
      end
    end
  end

  private

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::MysqlSelectInsertOutput).configure(conf)
  end
end
