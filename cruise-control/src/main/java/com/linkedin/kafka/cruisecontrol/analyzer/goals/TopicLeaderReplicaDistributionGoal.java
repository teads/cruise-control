/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 *
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingConstraint;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.model.ReplicaSortFunctionFactory;
import com.linkedin.kafka.cruisecontrol.model.SortedReplicasHelper;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.ACCEPT;
import static com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance.REPLICA_REJECT;
import static com.linkedin.kafka.cruisecontrol.analyzer.AnalyzerUtils.EPSILON;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.ReplicaDistributionAbstractGoal.ChangeType.*;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING;
import static com.linkedin.kafka.cruisecontrol.analyzer.goals.GoalUtils.replicaSortName;


/**
 * Soft goal to balance collocations of leader replicas of the same topic over alive brokers not excluded for replica moves.
 * <ul>
 * <li>Under: (the average number of topic leader replicas per broker) * (1 + topic replica count balance percentage)</li>
 * <li>Above: (the average number of topic leader replicas per broker) * Math.max(0, 1 - topic replica count balance percentage)</li>
 * </ul>
 *
 * @see com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#TOPIC_LEADER_REPLICA_COUNT_BALANCE_THRESHOLD_CONFIG
 * @see com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#GOAL_VIOLATION_DISTRIBUTION_THRESHOLD_MULTIPLIER_CONFIG
 * @see #balancePercentageWithMargin(OptimizationOptions)
 */
public class TopicLeaderReplicaDistributionGoal extends AbstractGoal {
  private static final Logger LOG = LoggerFactory.getLogger(TopicLeaderReplicaDistributionGoal.class);
  private static final double BALANCE_MARGIN = 0.9;
  // Flag to indicate whether the self healing failed to relocate all offline replicas away from dead brokers or broken
  // disks in its initial attempt and currently omitting the replica balance limit to relocate remaining replicas.
  private boolean _fixOfflineReplicasOnly;

  private final Map<String, Set<Integer>> _brokerIdsAboveBalanceUpperLimitByTopic;
  private final Map<String, Set<Integer>> _brokerIdsUnderBalanceLowerLimitByTopic;
  // Must contain only the topics to be rebalanced.
  private final Map<String, Double> _avgTopicLeadersOnAliveBroker;
  // Must contain all topics to ensure that the lower priority goals work w/o an NPE.
  private final Map<String, Integer> _balanceUpperLimitByTopic;
  private final Map<String, Integer> _balanceLowerLimitByTopic;
  // This is used to identify brokers not excluded for replica moves.
  private Set<Integer> _brokersAllowedReplicaMove;

  /**
   * A soft goal to balance collocations of leader replicas of the same topic.
   */
  public TopicLeaderReplicaDistributionGoal() {
    _brokerIdsAboveBalanceUpperLimitByTopic = new HashMap<>();
    _brokerIdsUnderBalanceLowerLimitByTopic = new HashMap<>();
    _avgTopicLeadersOnAliveBroker = new HashMap<>();
    _balanceUpperLimitByTopic = new HashMap<>();
    _balanceLowerLimitByTopic = new HashMap<>();
  }

  public TopicLeaderReplicaDistributionGoal(BalancingConstraint balancingConstraint) {
    this();
    _balancingConstraint = balancingConstraint;
  }

  /**
   * To avoid churns, we add a balance margin to the user specified rebalance threshold. e.g. when user sets the
   * threshold to be {@link BalancingConstraint#topicLeaderReplicaBalancePercentage()}, we use
   * ({@link BalancingConstraint#topicLeaderReplicaBalancePercentage()}-1)*{@link #BALANCE_MARGIN} instead.
   *
   * @param optimizationOptions Options to adjust balance percentage with margin in case goal optimization is triggered
   * by goal violation detector.
   * @return The rebalance threshold with a margin.
   */
  private double balancePercentageWithMargin(OptimizationOptions optimizationOptions) {
    double balancePercentage = optimizationOptions.isTriggeredByGoalViolation()
                               ? _balancingConstraint.topicLeaderReplicaBalancePercentage()
                                 * _balancingConstraint.goalViolationDistributionThresholdMultiplier()
                               : _balancingConstraint.topicLeaderReplicaBalancePercentage();

    return (balancePercentage - 1) * BALANCE_MARGIN;
  }

  /**
   * Ensure that the given balance limit falls into min/max limits determined by min/max gaps for topic replica balance.
   * If the computed balance limit is out of these gap-based limits, use the relevant max/min gap-based balance limit.
   *
   * @see com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#TOPIC_REPLICA_COUNT_BALANCE_MIN_GAP_DOC
   * @see com.linkedin.kafka.cruisecontrol.config.constants.AnalyzerConfig#TOPIC_REPLICA_COUNT_BALANCE_MAX_GAP_DOC
   *
   * @param computedLimit Computed balance upper or lower limit
   * @param average Average topic replicas on broker.
   * @return A balance limit that falls into [minGap, maxGap] for topic replica balance.
   */
  private int gapBasedBalanceLimit(int computedLimit, double average, boolean isLowerLimit) {
    int minLimit;
    int maxLimit;
    if (isLowerLimit) {
      maxLimit = Math.max(0, (int) (Math.floor(average) - _balancingConstraint.topicLeaderReplicaBalanceMinGap()));
      minLimit = Math.max(0, (int) (Math.floor(average) - _balancingConstraint.topicLeaderReplicaBalanceMaxGap()));
    } else {
      minLimit = (int) (Math.ceil(average) + _balancingConstraint.topicLeaderReplicaBalanceMinGap());
      maxLimit = (int) (Math.ceil(average) + _balancingConstraint.topicLeaderReplicaBalanceMaxGap());
    }
    return Math.max(minLimit, Math.min(computedLimit, maxLimit));
  }

  /**
   * @param topic Topic for which the upper limit is requested.
   * @param optimizationOptions Options to adjust balance upper limit in case goal optimization is triggered by goal
   * violation detector.
   * @return The topic replica balance upper threshold in number of topic replicas.
   */
  private int balanceUpperLimit(String topic, OptimizationOptions optimizationOptions) {
    int computedUpperLimit = (int) Math.ceil(_avgTopicLeadersOnAliveBroker.get(topic)
                                             * (1 + balancePercentageWithMargin(optimizationOptions)));
    return gapBasedBalanceLimit(computedUpperLimit, _avgTopicLeadersOnAliveBroker.get(topic), false);
  }

  /**
   * @param topic Topic for which the lower limit is requested.
   * @param optimizationOptions Options to adjust balance lower limit in case goal optimization is triggered by goal
   * violation detector.
   * @return The replica balance lower threshold in number of topic replicas.
   */
  private int balanceLowerLimit(String topic, OptimizationOptions optimizationOptions) {
    int computedLowerLimit = (int) Math.floor(_avgTopicLeadersOnAliveBroker.get(topic)
                                              * Math.max(0, (1 - balancePercentageWithMargin(optimizationOptions))));
    return gapBasedBalanceLimit(computedLowerLimit, _avgTopicLeadersOnAliveBroker.get(topic), true);
  }

  /**
   * Check whether the given action is acceptable by this goal. An action is acceptable if the number of topic replicas at
   * (1) the source broker does not go under the allowed limit -- unless the source broker is excluded for replica moves.
   * (2) the destination broker does not go over the allowed limit.
   *
   * @param action Action to be checked for acceptance.
   * @param clusterModel The state of the cluster.
   * @return {@link ActionAcceptance#ACCEPT} if the action is acceptable by this goal,
   * {@link ActionAcceptance#REPLICA_REJECT} otherwise.
   */
  @Override
  public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    String sourceTopic = action.topic();

    switch (action.balancingAction()) {
      case INTER_BROKER_REPLICA_SWAP:
        String destinationTopic = action.destinationTopic();
        if (sourceTopic.equals(destinationTopic)) {
          return ACCEPT;
        }

        // It is guaranteed that neither source nor destination brokers are excluded for replica moves.
        boolean acceptSourceToDest = (isLeadersCountUnderBalanceUpperLimitAfterChange(sourceTopic, destinationBroker, ADD)
                                      && isLeadersountAboveBalanceLowerLimitAfterChange(sourceTopic, sourceBroker, REMOVE));
        return (acceptSourceToDest
                && isLeadersCountUnderBalanceUpperLimitAfterChange(destinationTopic, sourceBroker, ADD)
                && isLeadersountAboveBalanceLowerLimitAfterChange(destinationTopic, destinationBroker, REMOVE)) ? ACCEPT
                                                                                                                 : REPLICA_REJECT;
      case LEADERSHIP_MOVEMENT: // FIXME: il ne faut pas accepter un changement de leadership à l'aveugle
        return ACCEPT;
      case INTER_BROKER_REPLICA_MOVEMENT:
        return (isLeadersCountUnderBalanceUpperLimitAfterChange(sourceTopic, destinationBroker, ADD)
                && (isExcludedForReplicaMove(sourceBroker)
                    || isLeadersountAboveBalanceLowerLimitAfterChange(sourceTopic, sourceBroker, REMOVE))) ? ACCEPT : REPLICA_REJECT;
      default:
        throw new IllegalArgumentException("Unsupported balancing action " + action.balancingAction() + " is provided.");
    }
  }

  private boolean isLeadersCountUnderBalanceUpperLimitAfterChange(String topic,
                                                                  Broker broker,
                                                                  ReplicaDistributionGoal.ChangeType changeType) {
    int numTopicLeaders = broker.numLeaderReplicasOfTopicInBroker(topic);
    int brokerBalanceUpperLimit = broker.isAlive() ? _balanceUpperLimitByTopic.get(topic) : 0;

    return changeType == ADD ? numTopicLeaders + 1 <= brokerBalanceUpperLimit : numTopicLeaders - 1 <= brokerBalanceUpperLimit;
  }

  private boolean isLeadersountAboveBalanceLowerLimitAfterChange(String topic,
                                                                 Broker broker,
                                                                 ReplicaDistributionGoal.ChangeType changeType) {
    int numTopicLeaders = broker.numLeaderReplicasOfTopicInBroker(topic);
    int brokerBalanceLowerLimit = broker.isAlive() ? _balanceLowerLimitByTopic.get(topic) : 0;

    return changeType == ADD ? numTopicLeaders + 1 >= brokerBalanceLowerLimit : numTopicLeaders - 1 >= brokerBalanceLowerLimit;
  }

  /**
   * Check whether the given broker is excluded for replica moves.
   * Such a broker cannot receive replicas, but can give them away.
   *
   * @param broker Broker to check for exclusion from replica moves.
   * @return {@code true} if the given broker is excluded for replica moves, {@code false} otherwise.
   */
  private boolean isExcludedForReplicaMove(Broker broker) {
    return !_brokersAllowedReplicaMove.contains(broker.id());
  }

  @Override
  public ClusterModelStatsComparator clusterModelStatsComparator() {
    return new TopicLeaderReplicaDistrGoalStatsComparator();
  }

  @Override
  public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
    return new ModelCompletenessRequirements(MIN_NUM_VALID_WINDOWS_FOR_SELF_HEALING, 0.0, true);
  }

  @Override
  public String name() {
    return TopicLeaderReplicaDistributionGoal.class.getSimpleName();
  }

  @Override
  public boolean isHardGoal() {
    return false;
  }

  /**
   * Initiates this goal.
   *
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    Set<String> topicsToRebalance = GoalUtils.topicsToRebalance(clusterModel, excludedTopics);
    if (topicsToRebalance.isEmpty()) {
      LOG.warn("All topics are excluded from {}.", name());
    }

    _brokersAllowedReplicaMove = GoalUtils.aliveBrokersNotExcludedForReplicaMove(clusterModel, optimizationOptions);
    if (_brokersAllowedReplicaMove.isEmpty()) {
      // Handle the case when all alive brokers are excluded from replica moves.
      throw new OptimizationFailureException("Cannot take any action as all alive brokers are excluded from replica moves.");
    }
    // Initialize the average replicas on an alive broker.
    for (String topic : clusterModel.topics()) {
      int numTopicLeaderReplicas = clusterModel.numTopicLeaderReplicas(topic);
      _avgTopicLeadersOnAliveBroker.put(topic, (numTopicLeaderReplicas / (double) _brokersAllowedReplicaMove.size()));
      _balanceUpperLimitByTopic.put(topic, balanceUpperLimit(topic, optimizationOptions));
      _balanceLowerLimitByTopic.put(topic, balanceLowerLimit(topic, optimizationOptions));
      // Retain only the topics to rebalance in _avgTopicLeadersOnAliveBroker
      if (!topicsToRebalance.contains(topic)) {
        _avgTopicLeadersOnAliveBroker.remove(topic);
      }
    }
    // Filter out replicas to be considered for replica movement.
    for (Broker broker : clusterModel.brokers()) {
      new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                       optimizationOptions.onlyMoveImmigrantReplicas())
                                .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrantOrOfflineReplicas(),
                                                       !clusterModel.selfHealingEligibleReplicas().isEmpty() && broker.isAlive())
                                .addSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics))
                                .trackSortedReplicasFor(replicaSortName(this, false, true), broker);
    }

    _fixOfflineReplicasOnly = false;
  }

  /**
   * Check if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   *
   * @param clusterModel The state of the cluster.
   * @param action Action containing information about potential modification to the given cluster model. Assumed to be
   * of type {@link ActionType#INTER_BROKER_REPLICA_MOVEMENT}.
   * @return True if requirements of this goal are not violated if this proposal is applied to the given cluster state,
   * false otherwise.
   */
  @Override
  protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
    Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
    // The action must be executed if currently fixing offline replicas only and the offline source replica is proposed
    // to be moved to another broker.
    if (_fixOfflineReplicasOnly && sourceBroker.replica(action.topicPartition()).isCurrentOffline()) {
      return action.balancingAction() == ActionType.INTER_BROKER_REPLICA_MOVEMENT;
    }

    //Check that destination and source would not become unbalanced.
    Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
    String sourceTopic = action.topic();

    return isLeadersCountUnderBalanceUpperLimitAfterChange(sourceTopic, destinationBroker, ADD) &&
           (isExcludedForReplicaMove(sourceBroker) || isLeadersountAboveBalanceLowerLimitAfterChange(sourceTopic, sourceBroker, REMOVE));
  }

  /**
   * Update goal state after one round of self-healing / rebalance.
   * @param clusterModel The state of the cluster.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
      throws OptimizationFailureException {
    if (!_brokerIdsAboveBalanceUpperLimitByTopic.isEmpty()) {
      _brokerIdsAboveBalanceUpperLimitByTopic.clear();
      _succeeded = false;
    }
    if (!_brokerIdsUnderBalanceLowerLimitByTopic.isEmpty()) {
      _brokerIdsUnderBalanceLowerLimitByTopic.clear();
      _succeeded = false;
    }
    // Sanity check: No self-healing eligible replica should remain at a dead broker/disk.
    try {
      GoalUtils.ensureNoOfflineLeaderReplicas(clusterModel, name());
    } catch (OptimizationFailureException ofe) {
      if (_fixOfflineReplicasOnly) {
        throw ofe;
      }
      _fixOfflineReplicasOnly = true;
      LOG.info("Ignoring topic leader replica balance limit to move leader replicas from dead brokers/disks.");
      return;
    }
    // Sanity check: Leader replica should not be moved to a broker, which used to host leader replica on its broken disk.
    GoalUtils.ensureLeaderReplicasMoveOffBrokersWithBadDisks(clusterModel, name());
    finish();
  }

  private static boolean skipBrokerRebalance(Broker broker,
                                             ClusterModel clusterModel,
                                             Collection<Replica> replicas,
                                             boolean requireLessReplicas,
                                             boolean requireMoreReplicas,
                                             boolean hasOfflineTopicReplicas,
                                             boolean moveImmigrantReplicaOnly) {
    if (broker.isAlive() && !requireMoreReplicas && !requireLessReplicas) {
      LOG.trace("Skip rebalance: Broker {} is already within the limit for leader replicas {}.", broker, replicas);
      return true;
    } else if (!clusterModel.newBrokers().isEmpty() && !broker.isNew() && !requireLessReplicas) {
      LOG.trace("Skip rebalance: Cluster has new brokers and this broker {} is not new, but does not require less load "
                + "for replicas {}.", broker, replicas);
      return true;
    }
    boolean hasImmigrantTopicReplicas = replicas.stream().anyMatch(replica -> broker.immigrantReplicas().contains(replica));
    if (!clusterModel.selfHealingEligibleReplicas().isEmpty() && requireLessReplicas
               && !hasOfflineTopicReplicas && !hasImmigrantTopicReplicas) {
      LOG.trace("Skip rebalance: Cluster is in self-healing mode and the broker {} requires less load, but none of its "
                + "current offline or immigrant replicas are from the topic being balanced {}.", broker, replicas);
      return true;
    } else if (moveImmigrantReplicaOnly && requireLessReplicas && !hasImmigrantTopicReplicas) {
      LOG.trace("Skip rebalance: Only immigrant replicas can be moved, but none of broker {}'s "
                + "current immigrant replicas are from the topic being balanced {}.", broker, replicas);
      return true;
    }

    return false;
  }

  private boolean isTopicExcludedFromRebalance(String topic) {
    return _avgTopicLeadersOnAliveBroker.get(topic) == null;
  }

  /**
   * Rebalance the given broker without violating the constraints of the current goal and optimized goals.
   *
   * @param broker         Broker to be balanced.
   * @param clusterModel   The state of the cluster.
   * @param optimizedGoals Optimized goals.
   * @param optimizationOptions Options to take into account during optimization.
   */
  @Override
  protected void rebalanceForBroker(Broker broker,
                                    ClusterModel clusterModel,
                                    Set<Goal> optimizedGoals,
                                    OptimizationOptions optimizationOptions) {
    LOG.debug("Rebalancing broker {} [limits] lower: {} upper: {}.", broker.id(), _balanceLowerLimitByTopic, _balanceUpperLimitByTopic);

    /**
     * FIXME: ConcurrentModificationException
     * java.util.ConcurrentModificationException
     * at java.base/java.util.HashMap$HashIterator.nextNode(HashMap.java:1493)
     * at java.base/java.util.HashMap$KeyIterator.next(HashMap.java:1516)
     * at [...].goals.TopicLeaderReplicaDistributionGoal.rebalanceForBroker(TopicLeaderReplicaDistributionGoal.java:399)
     * at [...].goals.AbstractGoal.optimize(AbstractGoal.java:81)
     * at com.linkedin.kafka.cruisecontrol.analyzer.GoalOptimizer.optimizations(GoalOptimizer.java:442)
     * at com.linkedin.kafka.cruisecontrol.KafkaCruiseControl.optimizations(KafkaCruiseControl.java:564)
     */
    for (String topic : broker.topics()) { // <-- ConcurrentModificationException (TopicLeaderReplicaDistributionGoal.java:399)
      if (isTopicExcludedFromRebalance(topic)) {
        continue;
      }

      Collection<Replica> topicLeaderReplicas = broker.leaderReplicasOfTopicInBroker(topic);
      int numTopicLeaderReplicas = topicLeaderReplicas.size();
      int numOfflineTopicLeaderReplicas = GoalUtils.retainCurrentOfflineBrokerReplicas(broker, topicLeaderReplicas).size();
      boolean isExcludedForReplicaMove = isExcludedForReplicaMove(broker);

      // Need more or less leaders?
      boolean requireLessLeaderReplicas = broker.isAlive()
          && numTopicLeaderReplicas > (isExcludedForReplicaMove ? 0 : _balanceUpperLimitByTopic.get(topic));
      boolean requireMoreLeaderReplicas = !isExcludedForReplicaMove && broker.isAlive()
          && numTopicLeaderReplicas < _balanceLowerLimitByTopic.get(topic);

      // Not related to leadership but useful in case of self healing
      boolean requireLessReplicasAsEmergency = _fixOfflineReplicasOnly && numOfflineTopicLeaderReplicas > 0;

      if (skipBrokerRebalance(broker, clusterModel, topicLeaderReplicas, requireLessLeaderReplicas, requireMoreLeaderReplicas,
          numOfflineTopicLeaderReplicas > 0, optimizationOptions.onlyMoveImmigrantReplicas())) {
        continue;
      }
      
      // Try to decrease leadership using leader movement if needed. If Leadership is still not satisfying, move some replicas
      if (((requireLessLeaderReplicas
          && rebalanceByMovingLeadershipOut(broker, topic, clusterModel, optimizedGoals, optimizationOptions))  
          || requireLessReplicasAsEmergency)
          && rebalanceByMovingReplicasOut(broker, topic, clusterModel, optimizedGoals, optimizationOptions)) {
        if (!requireLessReplicasAsEmergency) {
          _brokerIdsAboveBalanceUpperLimitByTopic.computeIfAbsent(topic, t -> new HashSet<>()).add(broker.id());
          LOG.debug("Failed to sufficiently decrease leader replica count in broker {} for topic {}. Leader replicas: {}.",
              broker.id(), topic, broker.numLeaderReplicasOfTopicInBroker(topic));
        }
     // Try to increase leadership using leader movement if needed. If Leadership is still not satisfying, move some replicas
      } else if (requireMoreLeaderReplicas
                 && rebalanceByMovingLeadershipIn(broker, topic, clusterModel, optimizedGoals, optimizationOptions)  
                 && rebalanceByMovingLeaderReplicasIn(broker, topic, clusterModel, optimizedGoals, optimizationOptions)) { 
        _brokerIdsUnderBalanceLowerLimitByTopic.computeIfAbsent(topic, t -> new HashSet<>()).add(broker.id());
        LOG.debug("Failed to sufficiently increase leader replica count in broker {} for topic {}. Leader replicas: {}.",
                  broker.id(), topic, broker.numLeaderReplicasOfTopicInBroker(topic));
      }
    }
  }

  /**
   * Try to decrease topic leadership on a specific broker using leader movement
   * 
   * @param broker Broker having to much leader replicas for the given topic
   * @param topic Topic name
   * @param clusterModel Model to optimize
   * @param optimizedGoals Already optimized goals
   * @param optimizationOptions Goal optimization options
   * @return True if the broker has still more leaders than permitted, false otherwise
   */
  private boolean rebalanceByMovingLeadershipOut(Broker broker,
                                                 String topic,
                                                 ClusterModel clusterModel,
                                                 Set<Goal> optimizedGoals,
                                                 OptimizationOptions optimizationOptions) {
    LOG.trace("Rebalance broker {} for topic {} by moving leadership out", broker, topic);
    
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    if (excludedTopics.contains(topic)) {
      LOG.trace("Skipping topic {} on broker {} the topic is in excluded list", topic, broker);
      return true;
    }
    
    // If the source broker is excluded for replica move, set its upper limit to 0.
    int balanceUpperLimitForTopic = isExcludedForReplicaMove(broker) ? 0 : _balanceUpperLimitByTopic.get(topic);
    int numTopicLeaderReplicas = broker.numLeaderReplicasOfTopicInBroker(topic);

    for (Replica leader : new HashSet<>(broker.leaderReplicasOfTopicInBroker(topic))) {
      // FIXME: Why excludedBrokersForLeadership is not used in LeaderReplicaDistributionGoal's candidate brokers filter?
      Set<Broker> candidateBrokers = clusterModel.partition(leader.topicPartition()).partitionBrokers().stream()
                                                 .filter(b -> b != broker && 
                                                         !b.replica(leader.topicPartition()).isCurrentOffline() &&
                                                         !optimizationOptions.excludedBrokersForLeadership().contains(b.id()))
                                                 .collect(Collectors.toSet());
      Broker b = maybeApplyBalancingAction(clusterModel,
                                           leader,
                                           candidateBrokers,
                                           ActionType.LEADERSHIP_MOVEMENT,
                                           optimizedGoals,
                                           optimizationOptions);
      // Only check if we successfully moved something.
      if (b != null) {
        if (--numTopicLeaderReplicas <= balanceUpperLimitForTopic) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean rebalanceByMovingLeadershipIn(Broker broker,
                                                String topic,
                                                ClusterModel clusterModel,
                                                Set<Goal> optimizedGoals,
                                                OptimizationOptions optimizationOptions) {
    LOG.trace("Rebalance broker {} for topic {} by moving leadership in", broker, topic);
    
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    if (excludedTopics.contains(topic)) {
      LOG.trace("Skipping topic {} on broker {} the topic is in excluded list", topic, broker);
      return true;
    }
    
    if (optimizationOptions.excludedBrokersForLeadership().contains(broker.id())) {
      LOG.trace("Skipping topic {} on broker {} because broker is in excluded for leadership list", broker);
      return true;
    }

    int numLeaderReplicas = broker.leaderReplicas().size();
    Set<Broker> candidateBrokers =  Collections.singleton(broker);

    for (Replica replica : broker.replicas()) {
      if (replica.isLeader() || replica.isCurrentOffline() || excludedTopics.contains(replica.topicPartition().topic())) {
        continue;
      }
      Broker b = maybeApplyBalancingAction(clusterModel,
                                           clusterModel.partition(replica.topicPartition()).leader(),
                                           candidateBrokers,
                                           ActionType.LEADERSHIP_MOVEMENT,
                                           optimizedGoals,
                                           optimizationOptions);
      // Only check if we successfully moved something.
      if (b != null) {
        if (++numLeaderReplicas >= _balanceLowerLimitByTopic.get(topic)) {
          return false;
        }
      }
    }
    return true;
  }
  

  /**
   * Evacuate some topic replicas from a given broker:
   * <ul> 
   * <li>Only topic <b>leader</b> replicas if there is no problem</li>
   * <li>Any topic replicas if self healing failed to relocate all offline replicas away from dead brokers or broken disks in its initial attempt</li>
   *
   * </ul>
   * 
   * @param broker Broker having to much leader replicas for the given topic
   * @param topic Topic name
   * @param clusterModel Model to optimize
   * @param optimizedGoals Already optimized goals
   * @param optimizationOptions Goal optimization options
   * @return True if it remains replicas of the given topic on the broker, 
   *         false if all the topic replicas has been moved away from the broker.
   */
  private boolean rebalanceByMovingReplicasOut(Broker broker,
                                               String topic,
                                               ClusterModel clusterModel,
                                               Set<Goal> optimizedGoals,
                                               OptimizationOptions optimizationOptions) {
    // Get the eligible brokers.
    SortedSet<Broker> candidateBrokers;
    
    if (_fixOfflineReplicasOnly) {
      candidateBrokers = new TreeSet<>(Comparator.comparingInt((Broker b) -> b.numReplicasOfTopicInBroker(topic))
                                                 .thenComparingInt(Broker::id));
      candidateBrokers.addAll(clusterModel.aliveBrokers());
    } else {
      candidateBrokers = new TreeSet<>(Comparator.comparingInt((Broker b) -> b.numLeaderReplicasOfTopicInBroker(topic))
                                                 .thenComparingInt(Broker::id));
      candidateBrokers.addAll(clusterModel.aliveBrokers()
                      .stream()
                      .filter(b -> b.numLeaderReplicasOfTopicInBroker(topic) < _balanceUpperLimitByTopic.get(topic))
                      .collect(Collectors.toSet()));
    }

    int balanceUpperLimit = _fixOfflineReplicasOnly ? 0 : _balanceUpperLimitByTopic.get(topic);
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    String replicaSortName = replicaSortName(this, false, !_fixOfflineReplicasOnly);
    new SortedReplicasHelper().maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectLeaders(), !_fixOfflineReplicasOnly)
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectOfflineReplicas(), _fixOfflineReplicasOnly)
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                     (!_fixOfflineReplicasOnly && !clusterModel.selfHealingEligibleReplicas().isEmpty())
                                                     || optimizationOptions.onlyMoveImmigrantReplicas())
                              .addSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics))
                              .trackSortedReplicasFor(replicaSortName, broker);
    SortedSet<Replica> candidateReplicas = broker.trackedSortedReplicas(replicaSortName).sortedReplicas(true);
    int numReplicas = candidateReplicas.size();
    for (Replica replica : candidateReplicas) {
      Broker b = maybeApplyBalancingAction(clusterModel,
                                           replica,
                                           candidateBrokers,
                                           ActionType.INTER_BROKER_REPLICA_MOVEMENT,
                                           optimizedGoals,
                                           optimizationOptions);
      // Only check if we successfully moved something.
      if (b != null) {
        if (--numReplicas <= balanceUpperLimit) {
          broker.untrackSortedReplicas(replicaSortName);
          return false;
        }
        // Remove and reinsert the broker so the order is correct.
        candidateBrokers.remove(b);
        if (b.numLeaderReplicasOfTopicInBroker(topic) < _balanceUpperLimitByTopic.get(topic) || _fixOfflineReplicasOnly) {
          candidateBrokers.add(b);
        }
      }
    }
    broker.untrackSortedReplicas(replicaSortName);
    return true;
  }
  

  private boolean rebalanceByMovingLeaderReplicasIn(Broker broker,
                                                    String topic,
                                                    ClusterModel clusterModel,
                                                    Set<Goal> optimizedGoals,
                                                    OptimizationOptions optimizationOptions) {
    if (optimizationOptions.excludedBrokersForLeadership().contains(broker.id())) {
      return true;
    }

    PriorityQueue<Broker> eligibleBrokers = new PriorityQueue<>(Comparator.comparingInt((Broker b) -> b.numLeaderReplicasOfTopicInBroker(topic))
                                                                          .thenComparingInt(Broker::id));
    eligibleBrokers.addAll(clusterModel.aliveBrokers()
                   .stream()
                   .filter(b -> b.numLeaderReplicasOfTopicInBroker(topic) > _balanceLowerLimitByTopic.get(topic))
                   .collect(Collectors.toSet()));
    List<Broker> candidateBrokers = Collections.singletonList(broker);
    Set<String> excludedTopics = optimizationOptions.excludedTopics();
    boolean onlyMoveImmigrantReplicas = optimizationOptions.onlyMoveImmigrantReplicas();
    String replicaSortName = replicaSortName(this, false, true);
    new SortedReplicasHelper().addSelectionFunc(ReplicaSortFunctionFactory.selectLeaders())
                              .maybeAddSelectionFunc(ReplicaSortFunctionFactory.selectImmigrants(),
                                                     !clusterModel.brokenBrokers().isEmpty() || onlyMoveImmigrantReplicas)
                              .addSelectionFunc(ReplicaSortFunctionFactory.selectReplicasBasedOnExcludedTopics(excludedTopics))
                              .trackSortedReplicasFor(replicaSortName, clusterModel);
    int numLeaderReplicas = broker.numLeaderReplicasOfTopicInBroker(topic);
    
    while (!eligibleBrokers.isEmpty()) {
      Broker sourceBroker = eligibleBrokers.poll();
      for (Replica replica : sourceBroker.trackedSortedReplicas(replicaSortName).sortedReplicas(true)) {
        Broker b = maybeApplyBalancingAction(clusterModel,
                                             replica,
                                             candidateBrokers,
                                             ActionType.INTER_BROKER_REPLICA_MOVEMENT,
                                             optimizedGoals,
                                             optimizationOptions);
        // Only need to check status if the action is taken. This will also handle the case that the source broker
        // has nothing to move in. In that case we will never reenqueue that source broker.
        if (b != null) {
          if (++numLeaderReplicas >= _balanceLowerLimitByTopic.get(topic)) {
            clusterModel.untrackSortedReplicas(replicaSortName);
            return false;
          }
          // If the source broker has a lower number of leader replicas than the next broker in the eligible broker
          // queue, we reenqueue the source broker and switch to the next broker.
          if (!eligibleBrokers.isEmpty() 
              && sourceBroker.numLeaderReplicasOfTopicInBroker(topic) < eligibleBrokers.peek().numLeaderReplicasOfTopicInBroker(topic)) {
            eligibleBrokers.add(sourceBroker);
            break;
          }
        }
      }
    }
    clusterModel.untrackSortedReplicas(replicaSortName);
    return true;
  }

  private class TopicLeaderReplicaDistrGoalStatsComparator implements ClusterModelStatsComparator {
      /**
     * 
     */
    private static final long serialVersionUID = -2166887342677287543L;
      private String _reasonForLastNegativeResult;

      @Override
      public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
        // Standard deviation of number of topic replicas over brokers not excluded for replica moves must be less than the
        // pre-optimized stats.
        double stdDev1 = stats1.topicLeaderReplicaStats().get(Statistic.ST_DEV).doubleValue();
        double stdDev2 = stats2.topicLeaderReplicaStats().get(Statistic.ST_DEV).doubleValue();
        int result = AnalyzerUtils.compare(stdDev2, stdDev1, EPSILON);
        if (result < 0) {
          _reasonForLastNegativeResult = String.format("Violated %s. [Std Deviation of Topic Leader Replica Distribution] post-"
                                                       + "optimization:%.3f pre-optimization:%.3f", name(), stdDev1, stdDev2);
        }
        return result;
      }

      @Override
      public String explainLastComparison() {
        return _reasonForLastNegativeResult;
      }
    }
}

