const sequelize = require('../database');
const { assert, Errors } = require('../utils/validator');
const rumSDK = require('rum-sdk-nodejs');
const Content = require('../database/sequelize/content');
const V1Content = require('../database/sequelize/v1Content');
const _ = require('lodash');

(async () => {
  await sequelize.authenticate();
  
  const [, , groupId, dryRun] = process.argv;
  console.log(`[Migrate v2 to v2]:`, { groupId, dryRun });

  assert(groupId, Errors.ERR_IS_REQUIRED('groupId'));

  const contentTrxIds = (await Content.findAll({
    attributes: ['TrxId'],
    where: { groupId },
    raw: true, 
    order: [
      ['id', 'ASC']
    ]
  })).map(c => c.TrxId);
  const v1ContentTrxIds = (await V1Content.findAll({
    attributes: ['trxId'],
    raw: true,
    order: [
      ['id', 'ASC']
    ]
  })).map(c => c.trxId);
  const diff = contentTrxIds.length - v1ContentTrxIds.length;
  console.log(`[summary]:`, { 
    'contentTrxIds count': contentTrxIds.length,
    'v1ContentTrxIds count': v1ContentTrxIds.length,
    'diff': contentTrxIds.length - _.intersection(contentTrxIds, v1ContentTrxIds).length,
  });

  if (diff === 0) {
    console.log('No data to migrate');
    return;
  }

  const v1ContentTrxIdSet = new Set(v1ContentTrxIds);
  for (const [index, contentTrxId] of Object.entries(contentTrxIds)) {
    if (!v1ContentTrxIdSet.has(contentTrxId)) {
      const content = await Content.findOne({ where: { TrxId: contentTrxId }, raw: true });
      console.log(`[Migrate content ${~~index + 1}]: ${contentTrxId} (id ${content.id})`);
      console.log(content.Data);
      if (!dryRun) {
        await V1Content.create({
          data: content.Data,
          trxId: content.TrxId,
          groupId: content.GroupId,
          raw: content,
          userAddress: rumSDK.utils.pubkeyToAddress(content.SenderPubkey),
          status: 'pending'
        });
      }
    }
  }
})();
