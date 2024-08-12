import axios from "axios";

import { MongoClient } from "mongodb";

const hackerNewsBaseUrl = "https://hacker-news.firebaseio.com/v0/";
const mongoUrl = "mongodb://localhost:27017/hacker-news";
/*
 * mongodb://swille:rpGo85CCgEP@docdb-2024-08-12-01-08-46.cluster-cqxqvilwbda9.us-east-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false
 * */
const DOCDB_URL =
  "mongodb://swille:rpGo85CCgEP@docdb-2024-08-12-01-08-46.cluster-cqxqvilwbda9.us-east-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false";

export const initApp: any = {};

initApp.testMongo = async () => {
  const client = await MongoClient.connect(mongoUrl);
  const dbo: any = client.db();
  const collections = await dbo.listCollections().toArray();
  // const collectionNames = collections.map((item) => item.name);

  return collections;
};
initApp.getHackerNewsData = async () => {
  console.log("START...", performance.now());
  ``;
  const topItems = await getTopItems();
  const storiesData: any = await getStoriesForTopItems(topItems.data);
  const stories: any = storiesData
    .filter((story) => story.status === "fulfilled")
    .map((story) => story.value.data);

  await saveAllToMongo("stories", stories);

  console.log("END...", performance.now());

  const storyComments = stories.flatMap((story) => story.kids);

  const commentsData = await getCommentsForStories(storyComments);

  const comments: any = commentsData
    .filter((comment) => comment.status === "fulfilled")
    .map((comment: any) => comment.value.data);

  await saveAllToMongo("comments", comments);
};

const getTopItems = async () => {
  return await axios.get(`${hackerNewsBaseUrl}/topstories.json`);
};

const getStoriesForTopItems = async (topItems) => {
  return await Promise.allSettled(
    topItems.map((itemId) =>
      axios.get(`${hackerNewsBaseUrl}/item/${itemId}.json`)
    )
  );
};

const getCommentsForStories = async (comments) => {
  return await Promise.allSettled(
    comments.map((commentId) =>
      axios.get(`${hackerNewsBaseUrl}/item/${commentId}.json`)
    )
  );
};

const saveAllToMongo = async (collectionName, documentsList) => {
  const client = await MongoClient.connect(mongoUrl);

  const dbo: any = client.db();

  const collections = await dbo.listCollections().toArray();
  const collectionNames = collections.map((item) => item.name);

  if (collectionNames.includes(collectionName)) {
    const drop = await dbo.collection(collectionName).drop();
  }

  await dbo.collection(collectionName).insertMany(documentsList);
  await client.close();
};
