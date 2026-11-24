package mk.ukim.rnmp;

public class GitHubEvent {
    public String userId;
    public String userLogin;
    public String repositoryId;
    public String repositoryName;
    public String type;
    public long timestamp;

    public GitHubEvent() {}

    public GitHubEvent(String userId, String userLogin, String repositoryId, String repositoryName, String type, long timestamp) {
        this.userId = userId;
        this.userLogin = userLogin;
        this.repositoryId = repositoryId;
        this.repositoryName = repositoryName;
        this.type = type;
        this.timestamp = timestamp;
    }
}
