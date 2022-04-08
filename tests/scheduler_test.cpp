#include <catch2/catch.hpp>
#include <iostream>

#include <treadmill/scheduler.h>

namespace treadmill {
namespace scheduler {
namespace test {

TEST_CASE("Identity group test.", "[scheduler]") {

  Application::logger = spdlog::default_logger();
  Node::logger = spdlog::default_logger();
  Event::logger = spdlog::default_logger();

  SECTION("op_test") {
    REQUIRE(all_lt(volume_t{1, 2, 3}, volume_t{2, 3, 4}));
    REQUIRE(!all_lt(volume_t{1, 2, 3}, volume_t{2, 2, 4}));

    REQUIRE(all_le(volume_t{1, 2, 3}, volume_t{1, 3, 4}));
    REQUIRE(!all_le(volume_t{1, 2, 3}, volume_t{1, 1, 4}));
  }

  SECTION("basic_test") {
    IdentityGroup empty("");
    REQUIRE(empty.acquire() == std::nullopt);

    IdentityGroup one("", 1);
    REQUIRE(one.acquire().value() == 0);
    REQUIRE(one.acquire() == std::nullopt);

    Identity i1{1};
    one.release(i1);
    REQUIRE(one.acquire() == std::nullopt);

    Identity i0{0};
    one.release(i0);
    REQUIRE(one.acquire().value() == 0);

    IdentityGroup ten("", 10);
    REQUIRE(ten.acquire().value() == 0);
    REQUIRE(ten.acquire().value() == 1);
    REQUIRE(ten.acquire().value() == 2);

    ten.release(i1);
    REQUIRE(ten.acquire() == 1);
  }

  SECTION("adjust_test") {
    IdentityGroup ten("", 10);
    REQUIRE(ten.acquire().value() == 0);

    ten.adjust(3);
    REQUIRE(ten.acquire().value() == 1);
    REQUIRE(ten.acquire().value() == 2);
    REQUIRE(!ten.acquire());

    ten.adjust(20);
    for (auto i = 0; i < 17; ++i) {
      REQUIRE(ten.acquire());
    }
  }
}

TEST_CASE("Application test.", "[scheduler]") {

  Application::logger = spdlog::default_logger();
  Node::logger = spdlog::default_logger();
  Event::logger = spdlog::default_logger();

  SECTION("identity_test_success") {
    auto ig = std::make_shared<IdentityGroup>("", 10);
    Application app("app1#0000000001", 1, volume_t{1, 1}, Affinity("app1"));
    app.identity_group(ig);
    REQUIRE(app.acquire_identity());
  }

  SECTION("identity_test_fail") {
    auto ig = std::make_shared<IdentityGroup>("", 0);
    Application app("app1#0000000001", 1, volume_t{1, 1}, Affinity("app1"));
    app.identity_group(ig);
    REQUIRE(!app.acquire_identity());
  }
}

TEST_CASE("Affinity counter test.", "[scheduler]") {

  Application::logger = spdlog::default_logger();
  Node::logger = spdlog::default_logger();
  Event::logger = spdlog::default_logger();

  SECTION("test_counters") {
    AffinityCounter ac;
    REQUIRE(ac.get("foo") == 0);
    ac.inc("foo");
    REQUIRE(ac.get("foo") == 1);
    ac.inc("foo");
    REQUIRE(ac.get("foo") == 2);
    ac.dec("foo");
    ac.dec("foo");
    REQUIRE(ac.get("foo") == 0);

    ac.inc("bla", 3);
    REQUIRE(ac.get("bla") == 3);
    ac.inc("bla", 3);
    REQUIRE(ac.get("bla") == 6);
  }
}

TEST_CASE("Node test.", "[scheduler]") {

  Application::logger = spdlog::default_logger();
  Node::logger = spdlog::default_logger();
  Event::logger = spdlog::default_logger();

  SECTION("test_level") {
    auto n0 = std::make_shared<Node>("n0");

    auto n1 = std::make_shared<Node>("n1");
    n0->add_node(n1);

    auto n2 = std::make_shared<Node>("n2");
    n1->add_node(n2);

    n0->apply_state();

    REQUIRE(n1->level() == 1);
    REQUIRE(n0->level() == 0);
    REQUIRE(n2->level() == 2);
  }

  SECTION("test_traits") {
    auto n0 = std::make_shared<Node>("n0");
    n0->traits(traits_t("001"));

    auto c1 = std::make_shared<Node>("c1");
    c1->traits(traits_t("100"));
    n0->add_node(c1);
    n0->apply_state();

    REQUIRE(n0->traits() == traits_t("101"));
    c1.reset();
    n0->apply_state();
    REQUIRE(n0->traits() == traits_t("001"));

    auto c2 = std::make_shared<Node>("c2");
    c2->traits(traits_t("000"));
    n0->add_node(c2);
    n0->apply_state();
    REQUIRE(n0->traits() == traits_t("001"));

    // Test that adding child node propagates recurisively to the parent.
    auto x1 = std::make_shared<Node>("x1");
    x1->traits(traits_t("100"));
    c2->add_node(x1);
    n0->apply_state();
    REQUIRE(n0->traits() == traits_t("101"));
    x1.reset();
    REQUIRE(n0->traits() == traits_t("101"));
    n0->apply_state();
    REQUIRE(n0->traits() == traits_t("001"));
  }

  SECTION("test_required_traits") {
    auto n0 = std::make_shared<Node>("n0");
    REQUIRE(n0->required_traits() == traits_t().set());

    using namespace std::chrono_literals;
    auto now = std::chrono::system_clock::now();

    auto s1 = std::make_shared<Server>("s1", "n0");
    s1->required_traits(traits_t("001"));
    s1->state(ServerState::up, now);
    n0->add_node(s1);
    n0->apply_state();
    REQUIRE(n0->required_traits() == traits_t("001"));

    auto s2 = std::make_shared<Server>("s2", "n0");
    REQUIRE(s2->required_traits() == traits_t());
    s2->state(ServerState::up, now);
    n0->add_node(s2);
    n0->apply_state();
    REQUIRE(n0->required_traits() == traits_t());
  }

  SECTION("test_valid_until") {
    using namespace std::chrono_literals;
    auto now = std::chrono::system_clock::now();

    auto n0 = std::make_shared<Node>("n0");
    n0->valid_until(now);

    auto c1 = std::make_shared<Node>("c1");
    c1->valid_until(now + 1h);
    n0->add_node(c1);
    n0->apply_state();
    REQUIRE(n0->valid_until() == (now + 1h));
    c1.reset();
    n0->apply_state();
    REQUIRE(n0->valid_until() == now);

    auto c2 = std::make_shared<Node>("c2");
    c2->valid_until(now);
    n0->add_node(c2);
    n0->apply_state();
    REQUIRE(n0->valid_until() == now);

    // Test that adding child node propagates recurisively to the parent.
    auto x1 = std::make_shared<Node>("x1");
    x1->valid_until(now + 20h);
    c2->add_node(x1);
    n0->apply_state();
    REQUIRE(n0->valid_until() == (now + 20h));
    x1.reset();
    n0->apply_state();
    REQUIRE(n0->valid_until() == now);
  }

  SECTION("test_volume_op") {
    volume_t v1 = {1, 2, 3};
    volume_t v2 = {1, 2, 3};
    volume_max_t combine;

    REQUIRE(all_eq(v1, v2));
    REQUIRE(all_eq(v1, {1, 2, 3}));
    combine(v1, {1, 2, 3});
    REQUIRE(all_eq(v1, {1, 2, 3}));
    combine(v1, {5, 1, 1});
    REQUIRE(all_eq(v1, {5, 2, 3}));
  }

  SECTION("test_capacity") {
    auto n0 = std::make_shared<Node>("");
    n0->capacity({0, 0, 0});

    auto c1 = std::make_shared<Node>("");
    c1->capacity({1, 2, 3});
    n0->add_node(c1);
    n0->apply_state();

    REQUIRE(all_eq(n0->capacity(), {1, 2, 3}));
    c1.reset();
    // Until state is applied, nothing propagates.
    REQUIRE(all_eq(n0->capacity(), {1, 2, 3}));
    n0->apply_state();
    REQUIRE(all_eq(n0->capacity(), {0, 0, 0}));

    auto c2 = std::make_shared<Node>("");
    c2->capacity({0, 0, 0});
    n0->add_node(c2);
    n0->apply_state();
    REQUIRE(all_eq(n0->capacity(), {0, 0, 0}));

    // Test that adding child node propagates recurisively to the parent.
    auto x1 = std::make_shared<Node>("");
    x1->capacity({3, 4, 5});
    c2->add_node(x1);
    n0->apply_state();
    REQUIRE(all_eq(n0->capacity(), {3, 4, 5}));
    REQUIRE(all_eq(c2->capacity(), {3, 4, 5}));

    auto x2 = std::make_shared<Node>("");
    x2->capacity({9, 1, 1});
    c2->add_node(x2);
    n0->apply_state();

    REQUIRE(all_eq(n0->capacity(), {9, 4, 5}));
    REQUIRE(all_eq(c2->capacity(), {9, 4, 5}));

    x1.reset();

    n0->apply_state();
    REQUIRE(all_eq(c2->capacity(), {9, 1, 1}));
    REQUIRE(all_eq(n0->capacity(), {9, 1, 1}));
  }

  SECTION("test_capacity_deltas") {
    auto n0 = std::make_shared<Node>("");
    n0->capacity({0, 0, 0});

    // Two intermediate nodes.
    auto c1 = std::make_shared<Node>("");
    c1->capacity({0, 0, 0});
    n0->add_node(c1);
    auto c2 = std::make_shared<Node>("");
    c2->capacity({0, 0, 0});
    n0->add_node(c2);

    auto c1_1 = std::make_shared<Node>("");
    c1_1->capacity({3, 4, 5});
    c1->add_node(c1_1);
    n0->apply_state();

    REQUIRE(all_eq(n0->capacity(), {3, 4, 5}));
    REQUIRE(all_eq(c1->capacity(), {3, 4, 5}));
    REQUIRE(all_eq(c2->capacity(), {0, 0, 0}));

    c1_1->inc_capacity({1, 1, 1});
    REQUIRE(all_eq(n0->capacity(), {4, 5, 6}));
    REQUIRE(all_eq(c1->capacity(), {4, 5, 6}));
    REQUIRE(all_eq(c2->capacity(), {0, 0, 0}));

    auto c1_2 = std::make_shared<Node>("");
    c1_2->capacity({1, 1, 1});
    c1->add_node(c1_2);
    n0->apply_state();

    // Nothing changed, since we added smaller node.
    REQUIRE(all_eq(n0->capacity(), {4, 5, 6}));
    REQUIRE(all_eq(c1->capacity(), {4, 5, 6}));

    c1_2->inc_capacity({1, 1, 1});
    REQUIRE(all_eq(n0->capacity(), {4, 5, 6}));
    REQUIRE(all_eq(c1->capacity(), {4, 5, 6}));

    c1_2->inc_capacity({3, 1, 1});

    // c1_2: 5,3,3
    // c1_1: 4,5,6
    // ---
    //       5,5,6
    REQUIRE(all_eq(n0->capacity(), {5, 5, 6}));
    REQUIRE(all_eq(c1->capacity(), {5, 5, 6}));

    auto c2_2 = std::make_shared<Node>("");
    c2_2->capacity({2, 2, 2});
    c2->add_node(c2_2);
    n0->apply_state();

    REQUIRE(all_eq(n0->capacity(), {5, 5, 6}));
    REQUIRE(all_eq(c2->capacity(), {2, 2, 2}));

    // Two c1_* will have {1,1,1} as result.
    c1_2->dec_capacity({4, 2, 2});
    c1_1->dec_capacity({3, 4, 5});

    REQUIRE(all_eq(c1_1->capacity(), {1, 1, 1}));
    REQUIRE(all_eq(c1_2->capacity(), {1, 1, 1}));
    REQUIRE(all_eq(c1->capacity(), {1, 1, 1}));
    REQUIRE(all_eq(c2->capacity(), {2, 2, 2}));
    REQUIRE(all_eq(n0->capacity(), {2, 2, 2}));
  }

  SECTION("test_affinity_counter") {
    auto n0 = std::make_shared<Node>("");
    auto n1 = std::make_shared<Node>("");
    n0->add_node(n1);
    n1->inc_affinity("foo");
    n0->apply_state();
    REQUIRE(n0->affinity("foo") == 1);
    REQUIRE(n1->affinity("foo") == 1);

    n1.reset();
    n0->apply_state();
    REQUIRE(n0->affinity("foo") == 0);

    auto n2 = std::make_shared<Node>("");
    n0->add_node(n2);
    auto x1 = std::make_shared<Node>("");
    auto x2 = std::make_shared<Node>("");
    n2->add_node(x1);
    n2->add_node(x2);
    n0->apply_state();

    x2->inc_affinity("bla");
    REQUIRE(x2->affinity("bla") == 1);
    REQUIRE(x1->affinity("bla") == 0);
    REQUIRE(n2->affinity("bla") == 1);
    REQUIRE(n0->affinity("bla") == 1);

    x1->inc_affinity("bla");
    x1->inc_affinity("bla");
    REQUIRE(x2->affinity("bla") == 1);
    REQUIRE(x1->affinity("bla") == 2);
    REQUIRE(n2->affinity("bla") == 3);
    REQUIRE(n0->affinity("bla") == 3);

    x1.reset();
    n0->apply_state();

    REQUIRE(x2->affinity("bla") == 1);
    REQUIRE(n2->affinity("bla") == 1);
    REQUIRE(n0->affinity("bla") == 1);
  }

  SECTION("test_labels") {
    auto n0 = std::make_shared<Node>("");
    // n0->labels({"b"});
    labels_t a_label{0x001};
    labels_t b_label{0x100};
    n0->labels(b_label);
    auto n1 = std::make_shared<Node>("");
    // n1->labels({"a"});
    n1->labels(a_label);
    n0->add_node(n1);
    n0->apply_state();
    REQUIRE(n0->has_labels(a_label));
    REQUIRE(n0->has_labels(b_label));
    REQUIRE(n1->has_labels(a_label));

    n1.reset();
    REQUIRE(n0->has_labels(a_label));
    n0->apply_state();
    REQUIRE(!n0->has_labels(a_label));
    REQUIRE(n0->has_labels(b_label));
  }

  SECTION("test_strategy") {
    auto n0 = std::make_shared<Node>("");

    auto c1 = std::make_shared<Node>("");
    auto c2 = std::make_shared<Node>("");
    auto c3 = std::make_shared<Node>("");

    n0->add_node(c1);
    n0->add_node(c2);
    n0->add_node(c3);

    SpreadStrategy spread(*n0);
    REQUIRE(spread.suggested() == c1);
    REQUIRE(spread.suggested() == c2);
    REQUIRE(spread.suggested() == c3);
    REQUIRE(spread.suggested() == c1);

    PackStrategy pack(*n0);
    REQUIRE(pack.suggested() == c1);
    REQUIRE(pack.suggested() == c1);
    REQUIRE(pack.next() == c2);
    REQUIRE(pack.suggested() == c2);

    c2.reset();
    n0->erase_expired();
    REQUIRE(spread.suggested() != c2);
    REQUIRE(pack.suggested() != c2);
  }
}

TEST_CASE("Allocation test.", "[scheduler]") {

  Application::logger = spdlog::default_logger();
  Node::logger = spdlog::default_logger();
  Event::logger = spdlog::default_logger();

  SECTION("test_priv_queue") {
    auto alloc = Allocation("alloc1", {10, 10});
    auto app1 = std::make_shared<Application>("app1#0000000001", 99,
                                              volume_t{1, 1}, Affinity("app1"));
    auto app2 = std::make_shared<Application>("app2#0000000002", 98,
                                              volume_t{2, 2}, Affinity("app2"));
    auto app3 = std::make_shared<Application>("app3#0000000003", 97,
                                              volume_t{3, 3}, Affinity("app3"));

    alloc.add(app1);
    alloc.add(app2);
    alloc.add(app3);

    auto priv_q = alloc.priv_queue();
    for (auto const &e : priv_q) {
      REQUIRE(e.rank == 100);
      REQUIRE(e.pending);
    }

    REQUIRE(priv_q[0].app->name() == "app1#0000000001");
    REQUIRE(priv_q[0].after == Approx(-0.9));

    REQUIRE(priv_q[1].app->name() == "app2#0000000002");
    REQUIRE(priv_q[1].after == Approx(-0.7));

    REQUIRE(priv_q[2].app->name() == "app3#0000000003");
    REQUIRE(priv_q[2].after == Approx(-0.4));
  }

  SECTION("test_queue") {
    auto alloc = Allocation("alloc1", {10, 10});

    auto app1 = std::make_shared<Application>("app1#0000000001", 10,
                                              volume_t{1, 1}, Affinity("app1"));
    auto app2 = std::make_shared<Application>("app2#0000000002", 50,
                                              volume_t{2, 2}, Affinity("app2"));
    auto app3 = std::make_shared<Application>("app3#0000000003", 99,
                                              volume_t{3, 3}, Affinity("app3"));

    alloc.add(app3);
    alloc.add(app2);
    alloc.add(app1);

    auto q = alloc.queue(volume_t{20, 20});
    REQUIRE(q.size() == 3);
    REQUIRE(q[0].app->name() == "app3#0000000003");
    REQUIRE(q[0].before == Approx(-10. / (10 + 20)));
    REQUIRE(q[0].after == Approx(-7. / (10 + 20)));

    REQUIRE(q[1].before == Approx(-7. / (10 + 20)));
    REQUIRE(q[1].after == Approx(-5. / (10 + 20)));

    REQUIRE(q[2].before == Approx(-5. / (10 + 20)));
    REQUIRE(q[2].after == Approx(-4. / (10 + 20)));
  }

  SECTION("test_no_reservation") {
    auto alloc = Allocation("alloc1", {0, 0});
    auto app1 = std::make_shared<Application>("app1#0000000001", 10,
                                              volume_t{1, 1}, Affinity("app1"));
    alloc.add(app1);
    auto q = alloc.queue(volume_t{10, 10});

    REQUIRE(q.size() == 1);
    REQUIRE(q[0].before == Approx(0. / 10));
    REQUIRE(q[0].after == Approx(1. / 10));
  }

  SECTION("test_sub_allocs") {
    auto alloc = std::make_shared<Allocation>("/a", volume_t{3, 3});
    auto queue = alloc->queue(volume_t{20, 20});

    auto sub_alloc_a = std::make_shared<Allocation>("/a/a", volume_t{5, 5});
    alloc->add_sub_alloc(sub_alloc_a);

    auto a1 = std::make_shared<Application>("1a#0000000001", 3, volume_t{2, 2},
                                            Affinity("1a"));
    auto a2 = std::make_shared<Application>("2a#0000000002", 2, volume_t{3, 3},
                                            Affinity("2a"));
    auto a3 = std::make_shared<Application>("3a#0000000003", 1, volume_t{5, 5},
                                            Affinity("3a"));

    sub_alloc_a->add(a1);
    sub_alloc_a->add(a2);
    sub_alloc_a->add(a3);
    auto priv_qa = sub_alloc_a->priv_queue();
    REQUIRE(priv_qa[0].app->name() == "1a#0000000001");

    queue = alloc->queue(volume_t{20, 20});
    REQUIRE(queue.size() == 3);
    REQUIRE(queue[0].app->name() == "1a#0000000001");
    REQUIRE(queue[0].after == Approx((2. - (5 + 3)) / (20 + (5 + 3))));

    auto sub_alloc_b = std::make_shared<Allocation>("/a/b", volume_t{10, 10});
    alloc->add_sub_alloc(sub_alloc_b);

    auto b1 = std::make_shared<Application>("1b#0000000004", 3, volume_t{2, 2},
                                            Affinity("1b"));
    auto b2 = std::make_shared<Application>("2b#0000000005", 2, volume_t{3, 3},
                                            Affinity("2b"));
    auto b3 = std::make_shared<Application>("3b#0000000006", 1, volume_t{5, 5},
                                            Affinity("3b"));

    sub_alloc_b->add(b1);
    sub_alloc_b->add(b2);
    sub_alloc_b->add(b3);
    auto priv_qb = sub_alloc_b->priv_queue();
    REQUIRE(priv_qb[0].app->name() == "1b#0000000004");

    queue = alloc->queue(volume_t{20, 20});
    REQUIRE(queue.size() == 6);

    REQUIRE(queue[0].app->name() == "1b#0000000004");
    REQUIRE(queue[0].after == Approx((2. - 18) / (20 + 18)));

    auto z1 = std::make_shared<Application>("1z#0000000007", 0, volume_t{2, 2},
                                            Affinity("1z"));
    auto z2 = std::make_shared<Application>("2z#0000000008", 0, volume_t{2, 2},
                                            Affinity("2z"));
    auto z3 = std::make_shared<Application>("3z#0000000009", 0, volume_t{2, 2},
                                            Affinity("3z"));

    alloc->add(z1);
    sub_alloc_a->add(z2);
    sub_alloc_b->add(z3);

    queue = alloc->queue(volume_t{20, 20});

    // Last (priority 0) three apps - 1z, 2z, 3z - in some order.
    REQUIRE(queue[queue.size() - 1].app->name()[1] == 'z');
    REQUIRE(queue[queue.size() - 2].app->name()[1] == 'z');
    REQUIRE(queue[queue.size() - 3].app->name()[1] == 'z');
  }

  SECTION("Test_sub_alloc_reservation") {
    auto alloc_a = std::make_shared<Allocation>("/a", volume_t{0, 0});
    auto alloc_b = std::make_shared<Allocation>("/a/b", volume_t{0, 0});
    alloc_a->add_sub_alloc(alloc_b);

    auto poor = std::make_shared<Allocation>("/a/b/poor", volume_t{0, 0});
    alloc_b->add_sub_alloc(poor);

    auto rich = std::make_shared<Allocation>("/a/b/rich", volume_t{5, 5});
    alloc_b->add_sub_alloc(rich);

    REQUIRE(rich->total_reserved()[0] == 5);
    REQUIRE(poor->total_reserved()[0] == 0);
    REQUIRE(alloc_b->total_reserved()[0] == 5);
    REQUIRE(alloc_a->total_reserved()[0] == 5);

    auto p1 = std::make_shared<Application>("p1#0000000001", 1, volume_t{1, 1},
                                            Affinity("p1"));
    auto r1 = std::make_shared<Application>("r1#0000000002", 1, volume_t{5, 5},
                                            Affinity("r1"));
    auto r2 = std::make_shared<Application>("r2#0000000003", 1, volume_t{5, 5},
                                            Affinity("r2"));
    poor->add(p1);
    rich->add(r1);
    rich->add(r2);

    auto queue = alloc_a->queue(volume_t{20, 20});
    REQUIRE(queue.size() == 3);
    REQUIRE(queue[0].app->name() == "r1#0000000002");
    REQUIRE(queue[1].app->name() == "p1#0000000001");
    REQUIRE(queue[2].app->name() == "r2#0000000003");
  }
}

TEST_CASE("server_test", "[scheduler]") {

  Application::logger = spdlog::default_logger();
  Node::logger = spdlog::default_logger();
  Event::logger = spdlog::default_logger();

  using namespace std::chrono_literals;
  auto now = std::chrono::system_clock::now();

  auto n0 = std::make_shared<Bucket>("n0", volume_t{0, 0, 0});
  n0->capacity({0, 0, 0});

  auto c1 = std::make_shared<Bucket>("c1", volume_t{0, 0, 0});
  c1->capacity({0, 0, 0});
  n0->add_node(c1);

  auto s1 = std::make_shared<Server>("s1", "c1");
  s1->state(ServerState::up);
  s1->capacity({10, 10, 10});
  s1->valid_until(now + 1h);
  c1->add_node(s1);
  n0->apply_state();

  REQUIRE(all_eq(s1->size(0), {10, 10, 10}));
  REQUIRE(all_eq(c1->size(0), {10, 10, 10}));
  REQUIRE(all_eq(n0->size(0), {10, 10, 10}));
  REQUIRE(all_eq(s1->capacity(), {10, 10, 10}));
  REQUIRE(all_eq(c1->capacity(), {10, 10, 10}));
  REQUIRE(all_eq(n0->capacity(), {10, 10, 10}));

  REQUIRE(s1->valid_until() == (now + 1h));
  REQUIRE(c1->valid_until() == (now + 1h));
  REQUIRE(n0->valid_until() == (now + 1h));

  n0->apply_state();

  SECTION("put") {
    auto app1 = std::make_shared<Application>(
        "app1#0000000001", 10, volume_t{1, 1, 1}, Affinity("app1"));
    REQUIRE(s1->check_app_constraints(app1, now) ==
            PlacementConstraints::success);
    REQUIRE(c1->check_app_constraints(app1, now) ==
            PlacementConstraints::success);
    REQUIRE(n0->check_app_constraints(app1, now) ==
            PlacementConstraints::success);
    REQUIRE(n0->put(app1, now));

    REQUIRE(all_eq(s1->capacity(), {9, 9, 9}));
    REQUIRE(all_eq(c1->capacity(), {9, 9, 9}));
    REQUIRE(all_eq(n0->capacity(), {9, 9, 9}));
    REQUIRE(s1->affinity("app1") == 1);
    REQUIRE(c1->affinity("app1") == 1);
    REQUIRE(n0->affinity("app1") == 1);
  }

  SECTION("direct_put") {
    auto app1 = std::make_shared<Application>(
        "app1#0000000001", 10, volume_t{1, 1, 1}, Affinity("app1"));
    REQUIRE(s1->put(app1, now));
    REQUIRE(all_eq(s1->capacity(), {9, 9, 9}));
    REQUIRE(all_eq(c1->capacity(), {9, 9, 9}));
    REQUIRE(all_eq(n0->capacity(), {9, 9, 9}));
    REQUIRE(s1->affinity("app1") == 1);
    REQUIRE(c1->affinity("app1") == 1);
    REQUIRE(n0->affinity("app1") == 1);
  }

  SECTION("capacity") {
    auto app1 = std::make_shared<Application>(
        "app1#0000000001", 10, volume_t{1, 11, 1}, Affinity("app1"));
    REQUIRE(!n0->put(app1, now));
  }

  SECTION("affinity") {
    auto app1 =
        std::make_shared<Application>("app1#0000000001", 10, volume_t{1, 1, 1},
                                      Affinity("app", {100, 100, 1}));
    auto app2 =
        std::make_shared<Application>("app2#0000000002", 10, volume_t{1, 1, 1},
                                      Affinity("app", {100, 100, 1}));

    REQUIRE(s1->affinity("app") == 0);
    REQUIRE(c1->affinity("app") == 0);
    REQUIRE(n0->affinity("app") == 0);

    REQUIRE(n0->put(app1, now));
    REQUIRE(!n0->put(app2, now));
    REQUIRE(!s1->put(app1, now));
    REQUIRE(!s1->put(app2, now));
  }

  SECTION("server_gone") {
    auto app1 =
        std::make_shared<Application>("app1#0000000001", 10, volume_t{1, 1, 1},
                                      Affinity("app", {100, 100, 1}));
    auto app2 =
        std::make_shared<Application>("app2#0000000002", 10, volume_t{1, 1, 1},
                                      Affinity("app", {100, 100, 1}));

    auto s2 = std::make_shared<Server>("s2", "c1");
    s2->state(ServerState::up);
    s2->capacity({10, 10, 10});
    s2->valid_until(now + 1h);
    c1->add_node(s2);

    n0->apply_state();

    REQUIRE(all_eq(c1->size(0), {20, 20, 20}));
    REQUIRE(all_eq(n0->size(0), {20, 20, 20}));

    REQUIRE(all_eq(n0->capacity(), {10, 10, 10}));
    REQUIRE(n0->put(app1, now));
    REQUIRE(all_eq(n0->capacity(), {10, 10, 10}));
    REQUIRE(n0->put(app2, now));
    REQUIRE(all_eq(n0->capacity(), {9, 9, 9}));

    s2.reset();

    REQUIRE(all_eq(c1->size(0), {10, 10, 10}));
    REQUIRE(all_eq(n0->size(0), {10, 10, 10}));

    REQUIRE(all_eq(n0->capacity(), {9, 9, 9}));
    REQUIRE(!n0->put(app2, now));
  }

  n0->apply_state();

  REQUIRE(all_eq(s1->capacity(), {10, 10, 10}));
  REQUIRE(all_eq(c1->capacity(), {10, 10, 10}));
  REQUIRE(all_eq(n0->capacity(), {10, 10, 10}));
  REQUIRE(s1->affinity("app") == 0);
  REQUIRE(c1->affinity("app") == 0);
  REQUIRE(n0->affinity("app") == 0);
}

TEST_CASE("application_test", "[scheduler]") {
  Application::logger = spdlog::default_logger();
  Node::logger = spdlog::default_logger();
  Event::logger = spdlog::default_logger();
}

TEST_CASE("cell_test", "[scheduler]") {

  Application::logger = spdlog::default_logger();
  Node::logger = spdlog::default_logger();
  Event::logger = spdlog::default_logger();

  using namespace std::chrono_literals;
  auto now = std::chrono::system_clock::now();

  auto cell = std::make_shared<Cell>("cell", 3);

  auto pod1 = std::make_shared<Bucket>("pod1", volume_t{0, 0, 0});
  auto pod2 = std::make_shared<Bucket>("pod2", volume_t{0, 0, 0});
  auto rack1 = std::make_shared<Bucket>("rack1", volume_t{0, 0, 0});
  auto rack2 = std::make_shared<Bucket>("rack2", volume_t{0, 0, 0});
  auto rack3 = std::make_shared<Bucket>("rack3", volume_t{0, 0, 0});
  auto rack4 = std::make_shared<Bucket>("rack4", volume_t{0, 0, 0});

  treadmill::scheduler::labels_t test_label{2};

  {
    // cell->add_partition("part1");
    cell->add_node(pod1);
    cell->add_node(pod2);
    REQUIRE(cell->children_.value().size() == 2);

    pod1->add_node(rack1);
    pod1->add_node(rack2);
    pod2->add_node(rack3);
    pod2->add_node(rack4);

    auto alloc =
        std::make_shared<Allocation>("alloc1", volume_t{0, 0, 0}, test_label);
    auto assignment = Assignment(std::regex(".*"), 1, alloc);
    cell->default_assignment(assignment);
    cell->allocations({alloc});
  }

  SECTION("empty_cell") {

    auto app1 = std::make_shared<Application>("app1#0000000001", 99,
                                              volume_t{1, 1}, Affinity("app1"));
    cell->assign(app1);
    cell->schedule();
    REQUIRE(app1->is_pending());
  }

  SECTION("simple_placement") {
    auto s1 = std::make_shared<Server>("s1", "rack1");
    s1->state(ServerState::up);
    s1->capacity(volume_t{10, 10, 10});
    s1->labels(test_label);
    s1->valid_until(now + 1h);
    rack1->add_node(s1);
    REQUIRE(s1->valid_until() == now + 1h);

    auto app1 = std::make_shared<Application>(
        "app1#0000000001", 99, volume_t{1, 1, 1}, Affinity("app1"));
    cell->assign(app1);
    cell->schedule();
    REQUIRE(app1->server().get() == s1.get());
    REQUIRE(s1.use_count() == 1);
  }

  SECTION("default_spread") {
    auto s1 = std::make_shared<Server>("s1", "rack1");
    s1->state(ServerState::up);
    s1->capacity(volume_t{10, 10, 10});
    s1->labels(test_label);
    s1->valid_until(now + 1h);
    rack1->add_node(s1);

    auto s2 = std::make_shared<Server>("s2", "rack2");
    s2->state(ServerState::up);
    s2->capacity(volume_t{10, 10, 10});
    s2->labels(test_label);
    s2->valid_until(now + 1h);
    rack2->add_node(s2);

    auto app1 = std::make_shared<Application>(
        "app1#0000000001", 99, volume_t{1, 1, 1}, Affinity("app"));
    auto app2 = std::make_shared<Application>(
        "app2#0000000002", 99, volume_t{1, 1, 1}, Affinity("app"));
    cell->assign(app1);
    cell->assign(app2);

    cell->schedule();
    REQUIRE(s1.use_count() == 1);
    REQUIRE(s2.use_count() == 1);

    REQUIRE(!app1->is_pending());
    REQUIRE(!app2->is_pending());
    REQUIRE(app1->server() != app2->server());

    s2.reset();
    cell->schedule();

    REQUIRE(!app1->is_pending());
    REQUIRE(!app2->is_pending());
    REQUIRE(app1->server() == app2->server());
  }

  SECTION("need_placement") {

    auto s1 = std::make_shared<Server>("s1", "rack1");
    s1->state(ServerState::up);
    s1->capacity(volume_t{10, 10, 10});
    s1->labels(test_label);
    s1->valid_until(now + 1h);
    rack1->add_node(s1);
    cell->apply_state();
    REQUIRE(s1->valid_until() == now + 1h);
    REQUIRE(cell->valid_until() == now + 1h);

    // TODO: should we consider special wrappers not to mix that two?
    auto lease_45min = 45min;
    auto data_retention_15min = 15min;

    auto app1 = std::make_shared<Application>(
        "app1#0000000001", 99, volume_t{1, 1, 1}, Affinity("app1"), lease_45min,
        data_retention_15min);
    cell->assign(app1);

    cell->schedule();
    REQUIRE(app1->is_scheduled());

    SECTION("blacklisted") {
      app1->blacklisted(true);
      // blacklisted apps do not need placement, but they release current
      // placement and identity.
      REQUIRE(!app1->need_placement(now));
      REQUIRE(app1->is_pending());
      // app->blacklisted(false);
      // cell->server("s1").value()->put(app);
    }

    SECTION("renew") {
      REQUIRE(!app1->need_placement(now));
      app1->renew(true);

      // Server will expire < in renew interval. Need to find new placement,
      // but the app does nto release the current placement.
      REQUIRE(app1->need_placement(now + 30min));
      REQUIRE(app1->is_scheduled());
      REQUIRE(app1->server().get() == s1.get());
    }

    SECTION("data_retention") {
      REQUIRE(!app1->need_placement(now));

      s1->state(ServerState::down, now);
      s1->apply_state();
      // Data retention - 15 min.
      REQUIRE(!app1->need_placement(now));
      REQUIRE(app1->server().get() == s1.get());
      REQUIRE(app1->need_placement(now + 16min));
      REQUIRE(!app1->server().get());
    }

    SECTION("server_gone") {
      REQUIRE(!app1->need_placement(now));
      s1.reset();
      REQUIRE(app1->need_placement(now));
    }

    app1.reset();
    s1.reset();
  }
}

TEST_CASE("feasibility_test", "[scheduler]") {
  Application::logger = spdlog::default_logger();
  Node::logger = spdlog::default_logger();
  Event::logger = spdlog::default_logger();

  affinity_limits_t limits(defaults::default_affinity_limit, 3);

  SECTION("same_app") {
    Application app1("app1#0000000001", 99, volume_t{1, 1},
                     Affinity("app", limits));
    Application app2("app2#0000000002", 99, volume_t{1, 1},
                     Affinity("app", limits));
    Application app3("app3#0000000003", 99, volume_t{1, 1},
                     Affinity("app3", limits));

    PlacementFeasibilityTracker tracker;

    REQUIRE(tracker.is_feasible(app1));
    tracker.record_failed(app1);
    REQUIRE(!tracker.is_feasible(app2));
    REQUIRE(tracker.is_feasible(app3));
  }

  SECTION("capacity") {
    Application app1("app1#0000000001", 99, volume_t{5, 5},
                     Affinity("app", limits));
    Application app2("app2#0000000002", 99, volume_t{6, 6},
                     Affinity("app", limits));
    Application app3("app3#0000000003", 99, volume_t{4, 6},
                     Affinity("app", limits));

    PlacementFeasibilityTracker tracker;

    tracker.record_failed(app1);
    REQUIRE(!tracker.is_feasible(app2));
    REQUIRE(tracker.is_feasible(app3));
  }

  SECTION("affinity") {
    Application app1("app1#0000000001", 99, volume_t{5, 5},
                     Affinity("app", {2, 2, 2}));
    Application app2("app2#0000000002", 99, volume_t{5, 5},
                     Affinity("app", {1, 2, 1}));
    Application app3("app3#0000000003", 99, volume_t{5, 5},
                     Affinity("app", {1, 3, 1}));

    PlacementFeasibilityTracker tracker;

    tracker.record_failed(app1);
    REQUIRE(!tracker.is_feasible(app2));
    REQUIRE(tracker.is_feasible(app3));
  }

  duration_t lease(0);
  duration_t data_retention(0);

  SECTION("traits") {
    Application app1("app1#0000000001", 99, volume_t{1, 1},
                     Affinity("app1", limits), lease, data_retention,
                     std::nullopt, std::nullopt, traits_t{1});
    Application app2("app2#0000000002", 99, volume_t{1, 1},
                     Affinity("app2", limits), lease, data_retention,
                     std::nullopt, std::nullopt, traits_t{2});

    PlacementFeasibilityTracker tracker;

    tracker.record_failed(app1);
    REQUIRE(tracker.is_feasible(app2));
  }

  SECTION("record_affinity_limits") {
    Application app1("app1#0000000001", 99, volume_t{5, 5},
                     Affinity("app", {2, 2, 2}));
    Application app2("app2#0000000002", 99, volume_t{5, 5},
                     Affinity("app", {2, 3, 2}));
    Application app3("app3#0000000003", 99, volume_t{5, 5},
                     Affinity("app", {1, 3, 4}));

    PlacementFeasibilityTracker tracker;
    REQUIRE(tracker.record_failed(app1));
    REQUIRE(tracker.record_failed(app2));
    REQUIRE(!tracker.record_failed(app3));
    REQUIRE(!tracker.record_failed(app1));
  }

  SECTION("record_demand") {
    Application app1("app1#0000000001", 99, volume_t{5, 5},
                     Affinity("app", {2, 2, 2}));
    Application app2("app2#0000000002", 99, volume_t{4, 5},
                     Affinity("app", {2, 2, 2}));
    Application app3("app3#0000000003", 99, volume_t{3, 3},
                     Affinity("app", {1, 2, 2}));

    PlacementFeasibilityTracker tracker;
    REQUIRE(tracker.record_failed(app1));
    REQUIRE(tracker.record_failed(app2));
    REQUIRE(!tracker.record_failed(app3));
    REQUIRE(!tracker.record_failed(app1));
  }
}

} // namespace test
} // namespace scheduler
} // namespace treadmill
