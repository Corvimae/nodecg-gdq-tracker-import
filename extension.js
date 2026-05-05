const fetch = require('isomorphic-fetch');
const { v4: uuid } = require('uuid');

module.exports = nodecg => {
  async function fetchFromTracker(baseURL, type, eventId) {
    const normalizedBaseURL = baseURL.endsWith('/') ? baseURL.substr(0, baseURL.length - 1) : baseURL;

    const request = await fetch(`${normalizedBaseURL}/api/v2/events/${eventId}/${type}.json`);
    
    try {
      const response = await request.json();

      if (request.status !== 200) throw new Error(`Error response from tracker: ${request.status} (${request.statusText}).`);
    
      if (response.next) return [...response.results, ...(await fetchFromTracker(response.next))];
    
      return response.results;
    } catch (e) {
      console.error(`Failed to fetch from ${path}:`);
      console.error(e);

      return [];
    }
  }

  function durationToSeconds(value) {
    if (!value) return 0;

    const sections = value.split(':');

    const seconds = Number(sections[sections.length - 1]);
    const minutes = Number(sections[sections.length - 2] || 0);
    const hours = Number(sections[sections.length - 3] || 0);

    return seconds + (minutes * 60) + (hours * 3600);
  }

  const gdqTrackerImportStatus = nodecg.Replicant('gdqTrackerImportStatus', {
    default: {
      isImporting: false,
      error: null,
      runsImported: null,
    },
  });

  const runDataArray = nodecg.Replicant('runDataArray', 'nodecg-speedcontrol');

  nodecg.listenFor('importGDQTrackerSchedule', async ({ trackerURL, eventID }, ack) => {
    nodecg.log.info('[GDQ Tracker Import] Schedule import started...');

    gdqTrackerImportStatus.value = {
      isImporting: true,
      error: null,
      runsImported: null,
    }

    try {
      const [runners, runs] = await Promise.all([
        fetchFromTracker(trackerURL, 'talent', eventID),
        fetchFromTracker(trackerURL, 'runs', eventID),
      ]);

      runDataArray.value = runs
        .filter(({ order }) => order !== null && order !== undefined)
        .map(run => {
          const matchesExistingRun = runDataArray.value.find(oldRun => oldRun.externalID === run.id.toString());

          const runData = {
            teams: [],
            id: (matchesExistingRun ? matchesExistingRun.id : null) || uuid(),
            externalID: run.id.toString(),
            customData: {},
          };

          runData.game = run.display_name || undefined;
          runData.system = run.console || undefined;
          runData.release = run.release_year?.toString() ?? undefined;
          runData.category = run.category || undefined;
          runData.estimate = run.run_time;
          runData.estimateS = durationToSeconds(run.run_time);
          runData.setupTime = run.setup_time;
          runData.setupTimeS = durationToSeconds(run.setup_time);
          runData.gameTwitch = run.twitch_name;
          runData.scheduled = run.starttime;
          runData.scheduledS = Math.floor(Date.parse(run.starttime) / 1000) + runData.setupTimeS + runData.estimateS;
          
          function buildTalentRecord(talentId, teamId) {
            const talentData = runners.find(({ id }) => id === talentId);
              
            if (!talentData) {
              nodecg.log.warn(`[GDQ Tracker Import] No talent data found for the talent with ID ${talentId}.`);

              return null;
            }

            return {
              id: uuid(),
              name: talentData.name,
              teamID: teamId,
              social: {
                twitch: talentData.stream ? talentData.stream.replace('http://twitch.tv/', '').replace('https://twitch.tv/', '').replace('twitch.tv/', '') : undefined,
              },
              pronouns: talentData.pronouns || undefined,
              customData: {},
            };
          }

          runData.teams = run.runners.map(({ id: runnerId }, index) => {
            const team = {
              id: uuid(),
              name: `Team ${index + 1}`,
              players: [],
            };

            const talentRecord = buildTalentRecord(runnerId, team.id);
            if (talentRecord) team.players.push(talentRecord);

            return team;
          });

          const commentatorTeam = {
            id: uuid(),
            name: 'Commentators',
            players: [],
          }

          for (const commentator of run.commentators) {
            const talentRecord = buildTalentRecord(commentator.id, commentatorTeam.id);
            if (talentRecord) commentatorTeam.players.push(talentRecord);
          }

          runData.teams.push(commentatorTeam);

          if (nodecg.bundleConfig.includeHosts) {
            const hostTeam = {
              id: uuid(),
              name: 'Hosts',
              players: [],
            }

            for (const host of run.hosts) {
              const talentRecord = buildTalentRecord(host.id, hostTeam.id);
              if (talentRecord) hostTeam.players.push(talentRecord);
            }

            runData.teams.push(hostTeam);
          }

          return runData;
        });

      gdqTrackerImportStatus.value = {
        isImporting: false,
        error: null,
        runsImported: runDataArray.value.length,
      }
  
      nodecg.log.info('[GDQ Tracker Import] Schedule import complete!');
      
      if (ack && !ack.handled) ack(null);
    } catch (error) {
      nodecg.log.warn('[GDQ Tracker Import] Schedule import failed:', error);

      gdqTrackerImportStatus.value = {
        isImporting: false,
        error: error.message || error.error || error.toString(),
        runsImported: null,
      }

      if (ack && !ack.handled) ack(error);
    }
  });
};