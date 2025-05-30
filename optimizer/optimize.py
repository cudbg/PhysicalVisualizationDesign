from search import SearchCandPlan
from z3 import *
from plan import *
import config
from multiprocessing import Pool

import time

def search_candidate_task(iact, view, binding, basic_plan):
    searcher = SearchCandPlan(basic_plan, iact)
    cands = searcher.search()
    return iact, view, binding, cands, searcher.unpruned_candidates

"""
Given a task, search for all candidate physical plans.

    return iact_cand_phys_plans, search_time
    
    iact_cand_phys_plans[iact][view][binding] = [plan1, plan2, ...]
"""
def search_candidates(task: Task):
    search_start = time.time()

    """
    For each view:
    - identify literal any nodes (all choices are literal)
    - push up non-literal any nodes

    return pushed_views, pushed_any_ids

    pushed_views[view] = pushed_up_plan
    pushed_any_ids[view] = [any_id ...] that are pushed up
    """
    pushed_views = {}
    pushed_any_ids = {}

    for view_idx, plan in enumerate(task.views):
        # find all non-literal any nodes
        def non_literal_anynode(n: Plan): return isinstance(n, AnyPlan)

        def non_literal_anyexpr(e: Expression):
            if isinstance(e, Any):
                return not all(map(lambda c: isinstance(c, C), e.choices))
            else:
                return False

        any_nodes = plan.find_nodes(non_literal_anynode)
        any_exprs = plan.find_exprs(non_literal_anyexpr)

        push_up_anys = {}
        for node in any_nodes:
            assert (isinstance(node, AnyPlan))
            push_up_anys[node.choice_id] = len(node.choices)
        for expr in any_exprs:
            assert (isinstance(expr, Any))
            push_up_anys[expr.choice_id] = len(expr.choices)

        push_up_anys = [(k, v) for k, v in push_up_anys.items()]

        # build a new plan that pushes up all non-literal any nodes
        def push_up_plan(ith_any, binding):
            nonlocal push_up_anys, plan
            if ith_any == len(push_up_anys):
                return plan.bind(binding)
            else:
                any_id, n_choices = push_up_anys[ith_any]
                sub_plans = []
                for j in range(n_choices):
                    binding[any_id] = j
                    sub_plans.append(push_up_plan(ith_any + 1, binding))
                return AnyPlan(any_id, sub_plans)

        pushed_views[view_idx] = push_up_plan(0, {})
        pushed_any_ids[view_idx] = list(map(lambda x: x[0], push_up_anys))

    """
    Figure out which interaction operates on which view, given (interaction, view) pair:
    For each basic plan of view, search for candidate plans
    
        iact_cand_phys_plans[iact][view][binding] = [plan1, plan2, ...] all plans that satisfy the interaction constraint
    """
    iact_cand_phys_plans = {}
    total_cands = 0

    search_tasks = []

    for iact in task.interactions:
        iact_cand_phys_plans[iact.name] = {}
        for view_idx, plan in enumerate(task.views):
            choice_nodes = set(iact.choice_nodes)
            # find if there exists a choice node or expr having the id pointed by the interaction
            if plan.find_nodes(lambda n: isinstance(n, ChoicePlan) and n.choice_id in choice_nodes) or \
                plan.find_exprs(lambda e: isinstance(e, ChoiceExpr) and e.choice_id in choice_nodes):
                # if so, the interaction can operate on the view
                iact_cand_phys_plans[iact.name][view_idx] = {}

                # find all basic plans of the view
                basic_plans = {}
                def find_basic_plan_root(plan: Plan, binding):
                    nonlocal basic_plans
                    if isinstance(plan, AnyPlan) and plan.choice_id in pushed_any_ids[view_idx]:
                        for i in range(len(plan.choices)):
                            binding[plan.choice_id] = i
                            find_basic_plan_root(plan.choices[i], binding)
                    else:
                        binding = tuple(sorted(list(binding.items())))
                        basic_plans[binding] = plan.clone()

                find_basic_plan_root(pushed_views[view_idx], {})

                for binding, basic_plan in basic_plans.items():
                    # if the current iact does not affect the current view, continue
                    choices_in_view = [e.choice_id for e in basic_plan.find_exprs(lambda e: isinstance(e, ChoiceExpr))]
                    if not set(iact.choice_nodes) & set(choices_in_view):
                        continue

                    search_tasks.append((iact, view_idx, binding, basic_plan))

    unpruned = 0
    for iact, view_idx, binding, basic_plan in search_tasks:
        _, _, _, iact_cand_phys_plans[iact.name][view_idx][binding], total = search_candidate_task(iact, view_idx, binding, basic_plan)
        unpruned += total
        if config.debug_candidate:
            for plan in iact_cand_phys_plans[iact.name][view_idx][binding]:
                print("="*50)
                print(plan)
        total_cands += len(iact_cand_phys_plans[iact.name][view_idx][binding])
    '''
    unpruned = 0
    with Pool(16) as p:
        for (iact, view_idx, binding, results, total) in p.starmap(search_candidate_task, search_tasks):
            unpruned += total
            if config.top_K_cands != 0:
                def total_memory(plan):
                    mem = 0
                    for node in plan.find_nodes(lambda n: isinstance(n, (SCache, DCache))):
                        mem += node.get_memory()
                    return mem
                results = sorted(results, key=total_memory)[:config.top_K_cands]
                print("cands after prune", len(results))
            iact_cand_phys_plans[iact.name][view_idx][binding] = results
            total_cands += len(iact_cand_phys_plans[iact.name][view_idx][binding])
    '''

    search_end = time.time()
    print("search ", total_cands, "/", unpruned, "candidates use ", search_end - search_start, "s")
    return iact_cand_phys_plans, search_end - search_start


"""
Given the constraints of the task and the candidate physical plans, solve the constraints to find the optimal physical plans.
if avg_time is None, does not consider avg_time
if avg_time is not None, only consider the plans that have average latency less than avg_time
if mem_ratio is None, set the memory constraint for client and memory respectively
if mem_ratio is not None, set the memory constraint to be mem_ratio less than the given value
"""
def solve_constraints(opt, iact_cand_phys_plans, task, avg_time=None, mem_ratio=None, verbose=False):
    if verbose:
        print("Solve", "avg_time", avg_time, "mem_ratio", mem_ratio)

    opt.reset()

    static_dss = {}
    dynamic_dss = {}

    client_static_mem = IntVal(0)
    server_static_mem = IntVal(0)
    max_server_dynamic_mem = Int("max_server_dynamic_mem")
    max_client_dynamic_mem = Int("max_client_dynamic_mem")

    for i, iact in enumerate(iact_cand_phys_plans):
        for v, view in enumerate(iact_cand_phys_plans[iact]):
            cand_plans = iact_cand_phys_plans[iact][view]
            for binding, cand_plan in cand_plans.items():
                # for each (interaction, view, basic_plan)
                # collect data structures for all candidate plans
                dss_cands = []
                for n, plan in enumerate(cand_plan):
                    cost = plan.cost(False)[0]
                    if avg_time is not None and cost.avg_latency >= avg_time:
                        continue
                    dss = []
                    static_ds = plan.find_nodes(lambda n: isinstance(n, SCache))
                    dynamic_ds = plan.find_nodes(lambda n: isinstance(n, DCache))
                    for d in static_ds:
                        sig = d.sig()
                        static_dss[sig] = d
                        dss.append(sig)
                        opt.add(And(Int(dss[-1]) >= 0, Int(dss[-1]) <= 1))
                    for d in dynamic_ds:
                        sig = d.sig()
                        dynamic_dss[sig] = d
                        dss.append(sig)
                        opt.add(And(Int(dss[-1]) >= 0, Int(dss[-1]) <= 1))
                    dss_cands.append(dss)
                if len(dss_cands) == 0:
                    return None

                '''
                construct constraint
                (1) plan1: ds1, ds2
                    plan2: ds3, ds4
                    ==> ds1 * ds2 + ds3 * ds4 >= 1
                (2) max_server_dynamic_mem >= sum(d_ds1, d_ds2, ...)
                '''
                valid_pickup = IntVal(0)
                server_dyn_mem = IntVal(0)
                client_dyn_mem = IntVal(0)
                for dss in dss_cands:
                    cand = IntVal(1)
                    for ds in dss:
                        cand = Int(ds) * cand
                        if ds.startswith("d_"):
                            node = dynamic_dss[ds]
                            mem = node.get_memory()
                            if node.at_server() == 1:
                                server_dyn_mem = server_dyn_mem + Int(ds) * mem
                            else:
                                client_dyn_mem = client_dyn_mem + Int(ds) * mem
                    valid_pickup = valid_pickup + cand

                opt.add(valid_pickup >= 1)  # (1)
                opt.add(max_server_dynamic_mem >= server_dyn_mem)  # (2)
                opt.add(max_client_dynamic_mem >= client_dyn_mem)  # (2)

    # constraints for static data structures
    for d, ds in static_dss.items():
        mem = ds.get_memory()
        if ds.at_server() == 1:
            server_static_mem = server_static_mem + Int(d) * mem
        else:
            client_static_mem = client_static_mem + Int(d) * mem

    server_mem = max_server_dynamic_mem + server_static_mem
    client_mem = max_client_dynamic_mem + client_static_mem

    if mem_ratio is not None:
        opt.add(server_mem / task.memory.server + client_mem / task.memory.client < mem_ratio)
    else:
        opt.add(server_mem * config.avg_alpha <= task.memory.server)
        opt.add(client_mem * config.avg_alpha <= task.memory.client)

    if verbose:
        print(time.time(), "start solving")

    if opt.check() == sat:
        return opt.model()
    else:
        return None


def optimize_for_server_memory(
    opt: Solver,
    task: Task,
    server_memory,
    iact_cand_phys_plans,
    find_arbitrary_valid_plan,
    only_for_valid_plan,
    verbose=False
):
    begin = time.time()
    task.memory.server = server_memory
    m = solve_constraints(opt, iact_cand_phys_plans, task, verbose=verbose)
    if m is not None:
        RESULT = "satisfiable"
        if find_arbitrary_valid_plan:
            avg_upper = 1E9
        else:
            last_avg = 0
            avg_upper = 100
            while solve_constraints(opt, iact_cand_phys_plans, task, avg_time=avg_upper, verbose=verbose) is None:
                last_avg = avg_upper
                avg_upper = avg_upper * 2
            avg_lower = last_avg
            while avg_upper - avg_lower > 1:
                avg = (avg_upper + avg_lower) / 2
                if solve_constraints(opt, iact_cand_phys_plans, task, avg_time=avg, verbose=verbose) is not None:
                    avg_upper = avg
                else:
                    avg_lower = avg
            m = solve_constraints(opt, iact_cand_phys_plans, task, avg_time=avg_upper, verbose=verbose)
    else:
        if only_for_valid_plan:
            return None
        RESULT = "unsatisfiable"
        last_ratio = 0
        mem_ratio_upper = 2
        while solve_constraints(opt, iact_cand_phys_plans, task, mem_ratio=mem_ratio_upper, verbose=verbose) is None and mem_ratio_upper < 1000:
            last_ratio = mem_ratio_upper
            mem_ratio_upper = mem_ratio_upper * 2
        if mem_ratio_upper > 1000:
            if verbose:
                print("No solution")
            return None
        mem_ratio_lower = last_ratio
        while mem_ratio_upper - mem_ratio_lower > 0.1:
            mem_ratio = (mem_ratio_upper + mem_ratio_lower) / 2
            if solve_constraints(opt, iact_cand_phys_plans, task, mem_ratio=mem_ratio, verbose=verbose) is not None:
                mem_ratio_upper = mem_ratio
            else:
                mem_ratio_lower = mem_ratio
        m = solve_constraints(opt, iact_cand_phys_plans, task, mem_ratio=mem_ratio_upper, verbose=verbose)
        avg_upper = 1E9

    print("search use ", time.time() - begin, "s")
    return m, avg_upper, RESULT


"""
Given a task, find optimal physical plans for each (interaction, view).
Arguments:
    task: 
        the Task to optimize
        
    (find_arbitrary_valid_plan, find_best_server_mem_give_upper_bound):
        (false, false) => default behavior. Find lowest average latency plans is possible, or find the best memory ratio plans
        (false, true) => binary search for lowest server memory that satisfy the memory constraint, and find the lowest average latency plan
        (true, false) => find any plan that satisfy the memory constraint
        (true, true) => binary search for lowest server memory that satisfy the memory constraint, and find any plan that satisfy the memory constraint
        
    only_for_valid_plan:
        if True, only find plans that satisfy the interaction constraints
        if False, also consider loose the constraints to find for best memory ratio
        ** if find_arbitrary_valid_plan is True, only_for_valid_plan is always True
    
    iact_cand_phys_plans: 
        precomputed candidate physical plans, 
        if None, search for candidates
        if not None, only use the given candidates
"""
def optimize(
        task: Task,
        find_arbitrary_valid_plan=False,
        find_best_server_mem_give_upper_bound=False,
        only_for_valid_plan=False,
        verbose=False
):
    print("Optimizing", task.memory, config.selectivity_mode, config.avg_alpha)
    iact_cand_phys_plans, search_time = search_candidates(task)

    opt_start = time.time()

    opt = Solver()
    opt.set("timeout", 6000000)

    result = {}

    if find_best_server_mem_give_upper_bound:
        server_upper_bound = task.memory.server
        server_lower_bound = 0
        while server_upper_bound - server_lower_bound > 1000:
            server_memory = (server_upper_bound + server_lower_bound) / 2
            print("Try Server memory", server_memory)
            res = optimize_for_server_memory(
                opt, task, server_memory, iact_cand_phys_plans,
                find_arbitrary_valid_plan=True,
                only_for_valid_plan=True,
                verbose=verbose)
            if res is not None:
                server_upper_bound = server_memory
            else:
                server_lower_bound = server_memory

        res = optimize_for_server_memory(
            opt, task, server_upper_bound, iact_cand_phys_plans,
            find_arbitrary_valid_plan=find_arbitrary_valid_plan,
            only_for_valid_plan=True,
            verbose=verbose)
        if res is None:
            result["result"] = "failed"
            return result
        m, avg_upper, RESULT = res
    else:
        res = optimize_for_server_memory(
            opt, task, task.memory.server, iact_cand_phys_plans,
            find_arbitrary_valid_plan=find_arbitrary_valid_plan,
            only_for_valid_plan=only_for_valid_plan,
            verbose=verbose)
        if res is None:
            result["result"] = "failed"
            return result
        m, avg_upper, RESULT = res


    iact_phys_plans = {}
    for i, iact in enumerate(iact_cand_phys_plans):
        for v, view in enumerate(iact_cand_phys_plans[iact]):
            cand_plans = iact_cand_phys_plans[iact][view]
            for b, (binding, cand_plan) in enumerate(cand_plans.items()):
                for n, plan in enumerate(cand_plan):
                    cost = plan.cost(False)[0]
                    if cost.avg_latency > avg_upper:
                        continue
                    ok = True
                    static_ds = plan.find_nodes(lambda n: isinstance(n, SCache))
                    dynamic_ds = plan.find_nodes(lambda n: isinstance(n, DCache))
                    for d in static_ds:
                        if m[Int(d.sig())].as_long() == 0:
                            ok = False
                            break
                    for d in dynamic_ds:
                        if m[Int(d.sig())].as_long() == 0:
                            ok = False
                            break
                    if ok:
                        iact_phys_plans[(iact, view, binding)] = plan

    opt_end = time.time()

    result["result"] = RESULT
    result["search_time"] = search_time
    result["z3_time"] = opt_end - opt_start

    n_candidates = 0
    candidate_static_ds = set()
    candidate_dynamic_ds = set()

    for iact in iact_cand_phys_plans:
        for view in iact_cand_phys_plans[iact]:
            for binding in iact_cand_phys_plans[iact][view]:
                n_candidates += len(iact_cand_phys_plans[iact][view][binding])
                for plan in iact_cand_phys_plans[iact][view][binding]:
                    static_ds = plan.find_nodes(lambda n: isinstance(n, SCache))
                    dynamic_ds = plan.find_nodes(lambda n: isinstance(n, DCache))
                    for d in static_ds:
                        candidate_static_ds.add(d.sig())
                    for d in dynamic_ds:
                        candidate_dynamic_ds.add(d.sig())
                    n_candidates += 1

    result["n_candidate_basic_plans"] = n_candidates
    result["n_candidate_scache"] = len(candidate_static_ds)
    result["n_candidate_dcache"] = len(candidate_dynamic_ds)

    used_static_ds = {}
    used_dynamic_ds = {}
    client_static_mem = 0
    server_static_mem = 0
    client_dynamic_mem = 0
    server_dynamic_mem = 0
    avg_time = {}
    for (i, v, b), plan in iact_phys_plans.items():
        used_dynamic_ds.setdefault(i, {})
        avg_time.setdefault(i, 0)
        cost = plan.cost(False)[0]
        if cost.avg_latency > avg_time[i]:
            avg_time[i] = cost.avg_latency
        static_ds = plan.find_nodes(lambda n: isinstance(n, SCache))
        dynamic_ds = plan.find_nodes(lambda n: isinstance(n, DCache))
        for d in static_ds:
            used_static_ds[d.sig()] = d
        for d in dynamic_ds:
            used_dynamic_ds[i][d.sig()] = d

    static_dss = []
    for d in used_static_ds.values():
        static_dss.append({"structure": str(d), "size": d.get_memory()})
        if d.at_server():
            server_static_mem += d.get_memory()
        else:
            client_static_mem += d.get_memory()
    result["static_data_structures"] = static_dss

    dynamic_dss = []
    for i, dss in used_dynamic_ds.items():
        client, server = 0, 0
        for d in dss.values():
            dynamic_dss.append({"structure": str(d), "size": d.get_memory()})
            if d.at_server():
                server += d.get_memory()
            else:
                client += d.get_memory()
        if client > client_dynamic_mem:
            client_dynamic_mem = client
        if server > server_dynamic_mem:
            server_dynamic_mem = server

    result["dynamic_data_structures"] = dynamic_dss

    result["client_static_mem"] = client_static_mem
    result["server_static_mem"] = server_static_mem
    result["client_dynamic_mem"] = client_dynamic_mem
    result["server_dynamic_mem"] = server_dynamic_mem
    result["total_client_mem"] = client_static_mem + client_dynamic_mem
    result["total_server_mem"] = server_static_mem + server_dynamic_mem
    result["max_avg_latency"] = max(list(avg_time.values()) + [0])
    result["plans"] = {}
    for (i, v, b), plan in iact_phys_plans.items():
        result["plans"][repr((i,v,b))] = {"str": str(plan), "json": plan.to_json()}

    return result
