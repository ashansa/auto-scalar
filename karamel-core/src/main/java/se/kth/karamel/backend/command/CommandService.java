/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.karamel.backend.command;

import com.google.gson.JsonObject;
import dnl.utils.text.table.TextTable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import se.kth.karamel.backend.ClusterDefinitionService;
import se.kth.karamel.backend.ClusterManager;
import se.kth.karamel.backend.ClusterService;
import se.kth.karamel.backend.LogService;
import se.kth.karamel.backend.converter.ChefJsonGenerator;
import se.kth.karamel.backend.converter.UserClusterDataExtractor;
import se.kth.karamel.backend.dag.Dag;
import se.kth.karamel.backend.launcher.amazon.Ec2Context;
import se.kth.karamel.backend.machines.MachinesMonitor;
import se.kth.karamel.backend.machines.SshMachine;
import se.kth.karamel.backend.machines.SshShell;
import se.kth.karamel.backend.machines.TaskSubmitter;
import se.kth.karamel.backend.mocking.MockingUtil;
import se.kth.karamel.backend.running.model.ClusterRuntime;
import se.kth.karamel.backend.running.model.Failure;
import se.kth.karamel.backend.running.model.GroupRuntime;
import se.kth.karamel.backend.running.model.MachineRuntime;
import se.kth.karamel.backend.running.model.tasks.DagBuilder;
import se.kth.karamel.backend.running.model.tasks.Task;
import se.kth.karamel.client.model.json.JsonCluster;
import se.kth.karamel.common.IoUtils;
import se.kth.karamel.common.SshKeyPair;
import se.kth.karamel.common.exception.KaramelException;

/**
 *
 * @author kamal
 */
public class CommandService {

  private static String chosenCluster = null;
  private static String autoselectedCluster = null;
  private static String chosenMachine = "";
  private static final ClusterService clusterService = ClusterService.getInstance();
  private static SshShell shell = null;
  private static String MENU_BAR = "";
  private static String HELP_PAGE_TEMPLATE = "";
  private static String HOME_PAGE_TEMPLATE = "";
  private static String YAMLS_TABLE_PLH = "%YAMLS_TABLE%";
  private static String CLUSTERS_TABLE_PLH = "%CLUSTERS_TABLE%";

  static {
    try {
      HELP_PAGE_TEMPLATE = IoUtils.readContentFromClasspath("se/kth/karamel/backend/command/helppage");
      HOME_PAGE_TEMPLATE = IoUtils.readContentFromClasspath("se/kth/karamel/backend/command/homepage");
    } catch (IOException e) {

    }
  }
//  private static final KaramelApi api = new KaramelApiImpl();

  public static CommandResponse processCommand(String command, String... args) throws KaramelException {
    String cmd = command.toLowerCase().trim();
    String nextCmd = null;
    CommandResponse.Renderer renderer = CommandResponse.Renderer.INFO;
    CommandResponse response = new CommandResponse();
    response.addMenuItem("Home", "home");
    response.addMenuItem("New Cluster", "new");
    response.addMenuItem("List Clusters", "list");
    selectCluster();
    String result = null;
    String successMessage = null;
    if (cmd.equals("help")) {
      result = HELP_PAGE_TEMPLATE;
    } else if (cmd.equals("home")) {
      result = HOME_PAGE_TEMPLATE.replace(YAMLS_TABLE_PLH, yamlsTable());
      result = result.replace(CLUSTERS_TABLE_PLH, clustersTable());
      nextCmd = "home";
    } else if (cmd.equals("clusters")) {
      result = clustersTable();
      nextCmd = "clusters";
    } else if (cmd.equals("list")) {
      StringBuilder builder = new StringBuilder();
      builder.append("Yaml Definitions:").append("\n").append("---------------------").append("\n");
      builder.append(yamlsTable());
      builder.append("\n").append("Running Clusters:").append("\n").append("----------------------").append("\n");
      builder.append(clustersTable());
      result = builder.toString();
      nextCmd = "list";
    } else if (cmd.equals("save")) {
      if (args.length == 0) {
        throw new KaramelException("Provide the yaml definition");
      }
      ClusterDefinitionService.saveYaml(args[0]);
      successMessage = "Yaml updated";
    } else if (cmd.equals("new")) {
      result = "";
      response.addMenuItem("Save", "save");
      renderer = CommandResponse.Renderer.YAML;
    } else {
      boolean found = false;
      Pattern p = Pattern.compile("use\\s+(\\w+)");
      Matcher matcher = p.matcher(cmd);
      if (!found && matcher.matches()) {
        found = true;
        String clusterName = matcher.group(1);
        if (cluster(clusterName) != null) {
          chosenCluster = clusterName;
          successMessage = String.format("switched to %s now", clusterName);
        } else {
          throw new KaramelException(String.format("cluster %s is not registered yet!!", clusterName));
        }
      }

      p = Pattern.compile("remove\\s+(\\w+)");
      matcher = p.matcher(cmd);
      if (!found && matcher.matches()) {
        found = true;
        String clusterName = matcher.group(1);
        if (cluster(clusterName) != null) {
          throw new KaramelException(String.format("%s is running now, terminate it first!!", clusterName));
        } else {
          ClusterDefinitionService.removeDefinition(clusterName);
          successMessage = String.format("cluster definition %s removed successfully..", clusterName);
        }
      }

      String clusterNameInUserInput = getClusterNameIfRunningAndMatchesForCommand(cmd, "yaml");
      if (!found && clusterNameInUserInput != null) {
        found = true;
        response.addMenuItem("Save", "save");
        renderer = CommandResponse.Renderer.YAML;
        result = ClusterDefinitionService.loadYaml(clusterNameInUserInput);
      }

      clusterNameInUserInput = getClusterNameIfRunningAndMatchesForCommand(cmd, "pause");
      if (!found && clusterNameInUserInput != null) {
        found = true;
        clusterService.pauseCluster(clusterNameInUserInput);
        successMessage = clusterNameInUserInput + " was scheduled for pausing, it might take some time please be patient!";
        nextCmd = "status " + clusterNameInUserInput;
      }

      clusterNameInUserInput = getClusterNameIfRunningAndMatchesForCommand(cmd, "resume");
      if (!found && clusterNameInUserInput != null) {
        found = true;
        clusterService.resumeCluster(clusterNameInUserInput);
        successMessage = clusterNameInUserInput + " was scheduled for resuming, it might take some time please be patient!";
        nextCmd = "status " + clusterNameInUserInput;
      }

      clusterNameInUserInput = getClusterNameIfRunningAndMatchesForCommand(cmd, "purge");
      if (!found && clusterNameInUserInput != null) {
        found = true;
        clusterService.purgeCluster(clusterNameInUserInput);
        successMessage = clusterNameInUserInput + " was scheduled for purging, it might take some time please be patient!";
        nextCmd = "status " + clusterNameInUserInput;
      }

      clusterNameInUserInput = getClusterNameIfRunningAndMatchesForCommand(cmd, "status");
      if (!found && clusterNameInUserInput != null) {
        found = true;
        addActiveClusterMenus(response);
        StringBuilder builder = new StringBuilder();
        ClusterManager cluster = cluster(clusterNameInUserInput);
        ClusterRuntime clusterEntity = cluster.getRuntime();
        builder.append(clusterEntity.getName()).append(" is ").append(clusterEntity.getPhase());
        if (clusterEntity.isPaused()) {
          builder.append(" but it is on pause.").append("\n");
        }

        if (clusterEntity.isFailed() && !clusterEntity.getFailures().isEmpty()) {
          builder.append(" List of failures: ").append("\n");
          builder.append(failureTable(clusterEntity.getFailures().values(), true));
        }
        builder.append("\n");
        builder.append(machinesTasksTable(clusterEntity));
        result = builder.toString();
        nextCmd = "status " + clusterNameInUserInput;
      }

      clusterNameInUserInput = getClusterNameIfRunningAndMatchesForCommand(cmd, "detail");
      if (!found && clusterNameInUserInput != null) {
        found = true;
        addActiveClusterMenus(response);
        ClusterManager cluster = cluster(clusterNameInUserInput);
        JsonCluster json = cluster.getDefinition();
        result = ClusterDefinitionService.serializeJson(json);
        nextCmd = "detail " + clusterNameInUserInput;
      }

      clusterNameInUserInput = getClusterNameIfRunningAndMatchesForCommand(cmd, "groups");
      if (!found && clusterNameInUserInput != null) {
        found = true;
        addActiveClusterMenus(response);
        ClusterManager cluster = cluster(clusterNameInUserInput);
        ClusterRuntime clusterEntity = cluster.getRuntime();
        String[] columnNames = {"Name", "Phase"};
        String[][] data = new String[clusterEntity.getGroups().size()][2];
        for (int i = 0; i < clusterEntity.getGroups().size(); i++) {
          GroupRuntime group = clusterEntity.getGroups().get(i);
          data[i][0] = group.getName();
          data[i][1] = String.valueOf(group.getPhase());
        }
        result = makeTable(columnNames, 0, data, true);
        nextCmd = "groups " + clusterNameInUserInput;
      }

      clusterNameInUserInput = getClusterNameIfRunningAndMatchesForCommand(cmd, "machines");
      if (!found && clusterNameInUserInput != null) {
        found = true;
        addActiveClusterMenus(response);
        ClusterManager cluster = cluster(clusterNameInUserInput);
        ClusterRuntime clusterEntity = cluster.getRuntime();
        ArrayList<MachineRuntime> machines = new ArrayList<>();
        for (GroupRuntime group : clusterEntity.getGroups()) {
          for (MachineRuntime machine : group.getMachines()) {
            machines.add(machine);
          }
        }
        result = machinesTable(machines, true);
        nextCmd = "machines " + clusterNameInUserInput;
      }

      clusterNameInUserInput = getClusterNameIfRunningAndMatchesForCommand(cmd, "tasks");
      if (!found && clusterNameInUserInput != null) {
        found = true;
        addActiveClusterMenus(response);
        ClusterManager cluster = cluster(clusterNameInUserInput);
        ClusterRuntime clusterEntity = cluster.getRuntime();
        result = machinesTasksTable(clusterEntity);
        nextCmd = "tasks " + clusterNameInUserInput;
      }

      p = Pattern.compile("shellconnect\\s+((\\d|.)+)");
      matcher = p.matcher(cmd);
      if (!found && matcher.matches()) {
        found = true;
        String ip = matcher.group(1);
        if (chosenCluster() != null) {
          String clusterName = chosenCluster();
          ClusterManager clusterMgr = cluster(clusterName);
          MachinesMonitor mm = clusterMgr.getMachinesMonitor();
          SshMachine machine = mm.getMachine(ip);
          if (machine != null) {
            shell = machine.getShell();
            if (shell.isConnected()) {
              throw new KaramelException("shell is already connected!!");
            } else {
              shell.connect();
              if (!shell.isConnected()) {
                throw new KaramelException("shell is not connected!!");
              } else {
                try {
                  Thread.sleep(200);

                } catch (InterruptedException ex) {
                  Logger.getLogger(CommandService.class
                          .getName()).log(Level.SEVERE, null, ex);
                }
                result = shell.readStreams();
                renderer = CommandResponse.Renderer.SSH;
                response.addMenuItem("Disconnect Shell", "shelldisconnect");
              }
            }
          } else {
            throw new KaramelException("Opps, machine was not found, make sure cluster is chosen first");
          }
        } else {
          throw new KaramelException("no cluster has been chosen yet!!");
        }
      }
      p = Pattern.compile("shelldisconnect");
      matcher = p.matcher(cmd);
      if (!found && matcher.matches()) {
        found = true;
        if (chosenCluster() != null) {
          if (shell != null) {
            shell.disconnect();
            successMessage = "Shell disconnected";
            renderer = CommandResponse.Renderer.INFO;
          } else {
            throw new KaramelException("Opps, there is not connected shell..");
          }
        } else {
          throw new KaramelException("no cluster has been chosen yet!!");
        }
      }

      p = Pattern.compile("shellexec(.+)");
      matcher = p.matcher(cmd);
      if (!found && matcher.matches()) {
        found = true;
        String cmdStr = matcher.group(1);
        if (chosenCluster() != null) {
          if (shell != null) {
            if (!shell.isConnected()) {
              throw new KaramelException("shell is not connected.");
            } else {
              shell.exec(cmdStr + "\r");
              try {
                Thread.sleep(100);

              } catch (InterruptedException ex) {
                Logger.getLogger(CommandService.class
                        .getName()).log(Level.SEVERE, null, ex);
              }
              result = shell.readStreams();
              renderer = CommandResponse.Renderer.SSH;
              response.addMenuItem("Disconnect Shell", "shelldisconnect");
            }
          } else {
            throw new KaramelException("Opps, there is not connected shell..");
          }
        } else {
          throw new KaramelException("no cluster has been chosen yet!!");
        }
      }

      p = Pattern.compile("shellread");
      matcher = p.matcher(cmd);
      if (!found && matcher.matches()) {
        found = true;
        if (chosenCluster() != null) {
          if (shell != null) {
            if (!shell.isConnected()) {
              throw new KaramelException("shell is not connected.");
            } else {
              result = shell.readStreams();
              renderer = CommandResponse.Renderer.SSH;
              response.addMenuItem("Disconnect Shell", "shelldisconnect");
            }
          } else {
            throw new KaramelException("Opps, there is not connected shell..");
          }
        } else {
          throw new KaramelException("no cluster has been chosen yet!!");
        }

      }

      p = Pattern.compile("dag\\s+(\\w+)");
      matcher = p.matcher(cmd);
      if (!found && matcher.matches()) {
        found = true;
        String clusterName = matcher.group(1);
        ClusterManager cluster = cluster(clusterName);
        if (cluster == null || cluster.getInstallationDag() == null) {
          TaskSubmitter dummyTaskSubmitter = new TaskSubmitter() {

            @Override
            public void submitTask(Task task) throws KaramelException {
              System.out.println(task.uniqueId());
              task.succeed();
            }
          };
          String yml = ClusterDefinitionService.loadYaml(clusterName);
          JsonCluster json = ClusterDefinitionService.yamlToJsonObject(yml);
          ClusterRuntime dummyRuntime = MockingUtil.dummyRuntime(json);
          Map<String, JsonObject> chefJsons = ChefJsonGenerator.generateClusterChefJsons(json, dummyRuntime);
          Dag installationDag = DagBuilder.getInstallationDag(json, dummyRuntime, dummyTaskSubmitter, chefJsons);
          installationDag.validate();
          result = installationDag.print();
        } else {
          result = cluster.getInstallationDag().print();
          addActiveClusterMenus(response);
          nextCmd = cmd;
        }

        renderer = CommandResponse.Renderer.INFO;
      }

      p = Pattern.compile("links\\s+(\\w+)");
      matcher = p.matcher(cmd);
      if (!found && matcher.matches()) {
        found = true;
        String clusterName = matcher.group(1);
        ClusterManager cluster = cluster(clusterName);
        if (cluster != null) {
          result = UserClusterDataExtractor.clusterLinks(cluster.getDefinition(), cluster.getRuntime());
          addActiveClusterMenus(response);
        } else {
          String yml = ClusterDefinitionService.loadYaml(clusterName);
          JsonCluster json = ClusterDefinitionService.yamlToJsonObject(yml);
          result = UserClusterDataExtractor.clusterLinks(json, null);
        }

        renderer = CommandResponse.Renderer.INFO;
      }

      p = Pattern.compile("launch\\s+(\\w+)");
      matcher = p.matcher(cmd);
      if (!found && matcher.matches()) {
        found = true;
        String clusterName = matcher.group(1);
        if (cluster(clusterName) != null) {
          throw new KaramelException(String.format("%s is already running, purge it first!!", clusterName));
        } else {
          String yaml = ClusterDefinitionService.loadYaml(clusterName);
          String json = ClusterDefinitionService.yamlToJson(yaml);
          clusterService.startCluster(json);
          successMessage = String.format("cluster %s launched successfully..", clusterName);
          nextCmd = "status";
        }
      }

      p = Pattern.compile("out\\s+(.*)");
      matcher = p.matcher(cmd);
      if (!found && matcher.matches()) {
        found = true;
        String taskuuid = matcher.group(1);
        if (chosenCluster() != null) {
          boolean taskFound = false;
          ClusterManager cluster = cluster(chosenCluster());
          ClusterRuntime clusterEntity = cluster.getRuntime();
          for (GroupRuntime group : clusterEntity.getGroups()) {
            for (MachineRuntime machine : group.getMachines()) {
              for (Task task : machine.getTasks()) {
                if (task.getUuid().equals(taskuuid)) {
                  taskFound = true;
                  result = LogService.loadOutLog(clusterEntity.getName(), machine.getPublicIp(), task.getName());
                }
              }
            }
          }
          if (!taskFound) {
            throw new KaramelException("Opps, task was not found, make sure cluster is chosen first");
          }
        } else {
          throw new KaramelException("no cluster has been chosen yet!!");
        }
      }

      p = Pattern.compile("err\\s+(.*)");
      matcher = p.matcher(cmd);
      if (!found && matcher.matches()) {
        found = true;
        String taskuuid = matcher.group(1);
        if (chosenCluster() != null) {
          boolean taskFound = false;
          ClusterManager cluster = cluster(chosenCluster());
          ClusterRuntime clusterEntity = cluster.getRuntime();
          for (GroupRuntime group : clusterEntity.getGroups()) {
            for (MachineRuntime machine : group.getMachines()) {
              for (Task task : machine.getTasks()) {
                if (task.getUuid().equals(taskuuid)) {
                  taskFound = true;
                  result = LogService.loadErrLog(clusterEntity.getName(), machine.getPublicIp(), task.getName());
                }
              }
            }
          }
          if (!taskFound) {
            throw new KaramelException("Opps, task was not found, make sure cluster is chosen first");
          }
        } else {
          throw new KaramelException("no cluster has been chosen yet!!");
        }
      }

      p = Pattern.compile("which\\s+(cluster|ec2|ssh)");
      matcher = p.matcher(cmd);
      if (!found && matcher.matches()) {
        found = true;
        String subcmd = matcher.group(1);
        if (subcmd.equals("cluster")) {
          if (chosenCluster() != null) {
            result = String.format("%s has been chosen.", chosenCluster());
          } else {
            throw new KaramelException("no cluster has been chosen yet!!");
          }
        } else if (subcmd.equals("ec2")) {
          Ec2Context ec2Context = clusterService.getCommonContext().getEc2Context();
          if (ec2Context != null) {
            result = String.format("ec2 account id is %s", ec2Context.getCredentials().getAccountId());
          } else {
            throw new KaramelException("no ec2 account has been chosen yet!!");
          }
        } else if (subcmd.equals("ssh")) {
          SshKeyPair sshKeyPair = clusterService.getCommonContext().getSshKeyPair();
          if (sshKeyPair != null) {
            result = String.format("public key path: %s \nprivate key path: %s", sshKeyPair.getPublicKeyPath(), sshKeyPair.getPrivateKeyPath());
          } else {
            throw new KaramelException("no ssh keys has been chosen yet!!");
          }
        }
      }
    }
    if (result == null && successMessage == null) {
      throw new KaramelException(String.format("Command '%s' not found", cmd));
    }
    response.setSuccessMessage(successMessage);
    response.addMenuItem("Help", "help");
    response.setNextCmd(nextCmd);
    response.setResult(result);
    response.setRenderer(renderer);
    return response;
  }

  private static String chosenCluster() {
    if (chosenCluster != null) {
      return chosenCluster;
    } else if (autoselectedCluster != null) {
      return autoselectedCluster;
    } else {
      return null;
    }
  }

  private static void selectCluster() {
    Map<String, ClusterManager> repository = clusterService.getRepository();
    if (autoselectedCluster != null && cluster(autoselectedCluster) == null) {
      autoselectedCluster = null;
    }
    if (chosenCluster != null && cluster(chosenCluster) == null) {
      chosenCluster = null;
    }

    if (chosenCluster == null && repository.size() == 1) {
      autoselectedCluster = (String) repository.keySet().toArray()[0];
    }
  }

  private static ClusterManager cluster(String name) {
    Map<String, ClusterManager> repository = clusterService.getRepository();
    Set<Map.Entry<String, ClusterManager>> clusters = repository.entrySet();
    for (Map.Entry<String, ClusterManager> cluster : clusters) {
      if (cluster.getKey().toLowerCase().equals(name.toLowerCase())) {
        return cluster.getValue();
      }
    }
    return null;
  }

  private static String clustersTable() {
    Map<String, ClusterManager> repository = clusterService.getRepository();
    String[] columnNames = {"Cluster Name", "Phase", "Failed/Paused", "Actions"};
    Object[][] data = new Object[repository.size()][columnNames.length];
    int i = 0;
    for (ClusterManager cluster : repository.values()) {
      String name = cluster.getDefinition().getName();
      data[i][0] = name;
      data[i][1] = cluster.getRuntime().getPhase();
      data[i][2] = cluster.getRuntime().isFailed() + "/" + cluster.getRuntime().isPaused();
      data[i][3] = "<a kref='status " + name + "'>status</a> <a kref='dag " + name + "'>dag</a> <a kref='groups " + name + "'>groups</a> <a kref='machines " + name + "'>machines</a> <a kref='tasks " + name + "'>tasks</a> <a kref='purge " + name + "'>purge</a> <a kref='links " + name + "'>links</a> <a kref='yaml " + name + "'>yaml</a>";
      i++;
    }

    return makeTable(columnNames, 1, data, true);
  }

  private static String yamlsTable() throws KaramelException {
    List<String> defs = ClusterDefinitionService.listClusters();
    String[] columnNames = {"Yaml Name", "Actions"};
    Object[][] data = new Object[defs.size()][columnNames.length];
    int i = 0;
    for (String yaml : defs) {
      data[i][0] = yaml;
      data[i][1] = "<a kref='yaml " + yaml + "'>edit</a> <a kref='dag " + yaml + "'>dag</a> <a kref='launch " + yaml + "'>launch</a> <a kref='remove " + yaml + "'>remove</a> <a kref='links " + yaml + "'>links</a>";
      i++;
    }

    return makeTable(columnNames, 1, data, true);
  }

  private static String failureTable(Collection<Failure> failures, boolean rowNumbering) {
    String[] columnNames = {"Failure", "Id", "Message"};
    Object[][] data = new Object[failures.size()][columnNames.length];
    int i = 0;
    for (Failure failure : failures) {
      data[i][0] = failure.getType().name();
      data[i][1] = (failure.getId() == null) ? "" : failure.getId();
      data[i][2] = failure.getMessage();
      i++;
    }

    return makeTable(columnNames, 1, data, rowNumbering);
  }

  private static String tasksTable(List<Task> tasks, boolean rowNumbering) {
    String[] columnNames = {"Task", "Status", "Machine", "Logs"};
    Object[][] data = new Object[tasks.size()][columnNames.length];
    for (int i = 0; i < tasks.size(); i++) {
      Task task = tasks.get(i);
      data[i][0] = task.getName();
      data[i][1] = task.getStatus();
      data[i][2] = task.getMachineId();
      String uuid = task.getUuid();
      data[i][3] = "<a kref='out " + uuid + "'>output</a> <a kref='err " + uuid + "'>error</a>";
    }
    return makeTable(columnNames, 1, data, rowNumbering);
  }

  private static String machinesTable(ArrayList<MachineRuntime> machines, boolean rowNumbering) {
    String[] columnNames = {"Machine", "Public IP", "Private IP", "SSH Port", "SSH User", "Life Status", "Task Status"};
    Object[][] data = new Object[machines.size()][columnNames.length];
    for (int i = 0; i < machines.size(); i++) {
      MachineRuntime machine = machines.get(i);
      data[i][0] = machine.getName();
      data[i][1] = "<a kref='shellconnect " + machine.getPublicIp() + "'>" + machine.getPublicIp() + "</a>";
      data[i][2] = machine.getPrivateIp();
      data[i][3] = machine.getSshPort();
      data[i][4] = machine.getSshUser();
      data[i][5] = machine.getLifeStatus();
      data[i][6] = machine.getTasksStatus();
    }
    return makeTable(columnNames, 6, data, rowNumbering);
  }

  private static String makeTable(String[] columnNames, int sortIndex, Object[][] data, boolean rowNumbering) {
    TextTable tt = new TextTable(columnNames, data);
// this adds the numbering on the left      
    tt.setAddRowNumbering(rowNumbering);
// sort by the first column                              
    tt.setSort(sortIndex);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(os);
    tt.printTable(ps, 0);
    ps.flush();
    return new String(os.toByteArray(), Charset.forName("utf8"));
  }

  private static String machinesTasksTable(ClusterRuntime clusterEntity) {
    StringBuilder builder = new StringBuilder();
    for (GroupRuntime group : clusterEntity.getGroups()) {
      for (MachineRuntime machine : group.getMachines()) {
        ArrayList<MachineRuntime> machines = new ArrayList<>();
        machines.add(machine);
        builder.append(machinesTable(machines, false));
        builder.append("\n");
        builder.append(tasksTable(machine.getTasks(), true));
        builder.append("\n");
      }
    }
    return builder.toString();
  }

  private static void addActiveClusterMenus(CommandResponse response) {
    ClusterManager cluster = cluster(chosenCluster());
    ClusterRuntime clusterEntity = cluster.getRuntime();
    response.addMenuItem("View Definition", "yaml");
    response.addMenuItem("Status", "status");
    response.addMenuItem("DAG", "dag");
    response.addMenuItem("Detail", "detail");
    response.addMenuItem("Groups", "groups");
    response.addMenuItem("Machines", "machines");
    response.addMenuItem("Tasks", "tasks");
    if (clusterEntity.isPaused()) {
      response.addMenuItem("Resume", "resume");
    } else {
      response.addMenuItem("Pause", "pause");
    }
    response.addMenuItem("Purge", "purge");

  }

  private static String getClusterNameIfRunningAndMatchesForCommand(String userinput, String cmd) throws KaramelException {
    Pattern p = Pattern.compile(cmd + "(\\s+(\\w+))?");
    Matcher matcher = p.matcher(userinput);
    if (matcher.matches()) {
      String clusterName;
      if (matcher.group(2) == null) {
        if (chosenCluster() != null) {
          ClusterManager cluster = cluster(chosenCluster());
          clusterName = cluster.getDefinition().getName();
        } else {
          throw new KaramelException("No cluster has been chosen yet! When you purge a cluster it is removed from the context.");
        }
      } else {
        clusterName = matcher.group(2);
      }
      return clusterName;
    } else {
      return null;
    }
  }

}
