module.exports = (content) => content.match(/(https?:\/\/)([\w&@.:/?=-]+)/g) || [];